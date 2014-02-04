package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

public class DbLedgerStorage implements CompactableLedgerStorage {

    private EntryLogger entryLogger;

    private LedgerMetadataIndex ledgerIndex;
    private EntryLocationIndex entryLocationIndex;

    private GarbageCollectorThread gcThread;

    private ConcurrentNavigableMap<LongPair, ByteBuffer> writeCache = new ConcurrentSkipListMap<LongPair, ByteBuffer>();
    private ConcurrentNavigableMap<LongPair, ByteBuffer> writeCacheBeingFlushed;

    private final AtomicLong writeCacheSize = new AtomicLong(0);
    private final AtomicLong writeCacheSizeTrimmed = new AtomicLong(0);
    private final ReentrantReadWriteLock writeCacheMutex = new ReentrantReadWriteLock();

    private final WriteCache cache = new WriteCache();

    private final RateLimiter writeRateLimiter = RateLimiter.create(5000);
    private final RateLimiter readRateLimiter = RateLimiter.create(100);
    private final AtomicBoolean isThrottlingRequests = new AtomicBoolean(false);

    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
            "db-storage-%s").build());

    private static final String WRITE_CACHE_MAX_SIZE = "writeCacheMaxSize";

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE = 4 * 1024 * 1024 * 1024L;

    private long writeCacheMaxSize;

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager)
            throws IOException {
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

        writeCacheMaxSize = conf.getLong(WRITE_CACHE_MAX_SIZE, DEFAULT_WRITE_CACHE_MAX_SIZE);

        ledgerIndex = new LedgerMetadataIndex(baseDir);
        entryLocationIndex = new EntryLocationIndex(baseDir);

        entryLogger = new EntryLogger(conf, ledgerDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerManager, this);
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        try {
            gcThread.shutdown();
            entryLogger.shutdown();

            ledgerIndex.close();
            entryLocationIndex.close();

            executor.shutdown();

        } catch (IOException e) {
            log.error("Error closing db storage", e);
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        LedgerData ledgerData = ledgerIndex.get(ledgerId);
        log.debug("Ledger exists. ledger: {} : {}", ledgerId, ledgerData.getExists());
        return ledgerData.getExists();
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        log.debug("Set fenced. ledger: {}", ledgerId);
        LedgerData ledgerData = ledgerIndex.get(ledgerId);
        if (ledgerData.getFenced()) {
            return false;
        } else {
            ledgerData = LedgerData.newBuilder(ledgerData).setFenced(true).build();
            ledgerIndex.set(ledgerId, ledgerData);
            return true;
        }
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        log.debug("isFenced. ledger: {}", ledgerId);
        return ledgerIndex.get(ledgerId).getFenced();
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        log.debug("Set master key. ledger: {}", ledgerId);
        LedgerData.Builder ledgerData = LedgerData.newBuilder(ledgerIndex.get(ledgerId));
        ledgerData.setMasterKey(ByteString.copyFrom(masterKey));
        ledgerIndex.set(ledgerId, ledgerData.build());
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        log.debug("Read master key. ledger: {}", ledgerId);
        return ledgerIndex.get(ledgerId).getMasterKey().toByteArray();
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        log.debug("Add entry. {}@{}", ledgerId, entryId);

        long cacheSize;

        if (isThrottlingRequests.get()) {
            writeRateLimiter.acquire();
        }

        ByteBuffer cachedEntry = cache.addEntry(entry);

        writeCacheMutex.readLock().lock();
        try {
            writeCache.put(new LongPair(ledgerId, entryId), cachedEntry);
            cacheSize = writeCacheSize.addAndGet(entry.remaining());
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        if (cacheSize > writeCacheMaxSize && isThrottlingRequests.compareAndSet(false, true)) {
            // Trigger an early flush in background
            executor.submit(new Runnable() {
                public void run() {
                    try {
                        flush();
                    } catch (IOException e) {
                        log.error("Error during flush", e);
                    }
                }
            });
        }

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        log.debug("Get Entry: {}@{}", ledgerId, entryId);
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntry(ledgerId);
        }

        LongPair entryKey = new LongPair(ledgerId, entryId);

        writeCacheMutex.readLock().lock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuffer entry = writeCache.get(entryKey);
            if (entry != null) {
                return entry.duplicate();
            }

            // If there's a flush going on, the entry might be in the flush buffer
            if (writeCacheBeingFlushed != null) {
                entry = writeCacheBeingFlushed.get(entryKey);
                if (entry != null) {
                    return entry.duplicate();
                }
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // When reading from db we want to avoid that too many reads to affect the flush time
        if (isThrottlingRequests.get()) {
            readRateLimiter.acquire();
        }

        // Read from main storage
        try {
            long entryLocation = entryLocationIndex.getLocation(ledgerId, entryId);
            byte[] content = entryLogger.readEntry(ledgerId, entryId, entryLocation);
            return ByteBuffer.wrap(content);
        } catch (NoEntryException e) {
            if (ledgerExists(ledgerId)) {
                // We couldn't find the entry and we have other entries past that one entry, we can assume the entry was
                // already trimmed by the client
                throw new Bookie.EntryTrimmedException(e.getLedger(), e.getEntry());
            } else {
                // It was really a NoEntry error
                throw e;
            }
        }
    }

    public ByteBuffer getLastEntry(long ledgerId) throws IOException {
        // Next entry key is the first entry of the next ledger, we first seek to that entry and then step back to find
        // the last entry on ledgerId
        LongPair nextEntryKey = new LongPair(ledgerId + 1, 0);

        writeCacheMutex.readLock().lock();
        try {
            // First try to read from the write cache of recent entries
            Entry<LongPair, ByteBuffer> mapEntry = writeCache.headMap(nextEntryKey).lastEntry();
            if (mapEntry != null && mapEntry.getKey().first == ledgerId) {
                return mapEntry.getValue();
            }

            // If there's a flush going on, the entry might be in the flush buffer
            if (writeCacheBeingFlushed != null) {
                mapEntry = writeCacheBeingFlushed.headMap(nextEntryKey).lastEntry();
                if (mapEntry != null && mapEntry.getKey().first == ledgerId) {
                    return mapEntry.getValue();
                }
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // Search the last entry in storage
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        byte[] content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);
        return ByteBuffer.wrap(content);
    }

    @Override
    public boolean isFlushRequired() {
        writeCacheMutex.readLock().lock();
        try {
            return !writeCache.isEmpty();
        } finally {
            writeCacheMutex.readLock().unlock();
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        ConcurrentNavigableMap<LongPair, ByteBuffer> newWriteCache = new ConcurrentSkipListMap<LongPair, ByteBuffer>();

        writeCacheMutex.writeLock().lock();

        long sizeToFlush;
        long sizeTrimmed;
        try {
            // First, swap the current write-cache map with an empty one so that writes will go on unaffected
            cache.startFlush();

            // Only a single flush is happening at the same time
            checkArgument(writeCacheBeingFlushed == null);

            writeCacheBeingFlushed = writeCache;
            writeCache = newWriteCache;

            sizeTrimmed = writeCacheSizeTrimmed.getAndSet(0);
            sizeToFlush = writeCacheSize.getAndSet(0) - sizeTrimmed;

            // Write cache is empty now, so we can accept writes at full speed again
            isThrottlingRequests.set(false);
        } finally {
            writeCacheMutex.writeLock().unlock();
        }

        log.info("Flushing entries. size {} Mb -- Already trimmed: {} Mb", sizeToFlush / 1024.0 / 1024,
                sizeTrimmed / 1024.0 / 1024);
        long start = System.nanoTime();

        // Write all the pending entries into the entry logger and collect the offset position for each entry
        Multimap<Long, LongPair> locationMap = ArrayListMultimap.create();
        for (Entry<LongPair, ByteBuffer> entry : writeCacheBeingFlushed.entrySet()) {
            LongPair ledgerAndEntry = entry.getKey();
            ByteBuffer content = entry.getValue();
            long ledgerId = ledgerAndEntry.first;
            long entryId = ledgerAndEntry.second;

            long location = entryLogger.addEntry(ledgerId, content.duplicate());
            locationMap.put(ledgerId, new LongPair(entryId, location));
        }

        cache.doneFlush();

        entryLogger.flush();

        entryLocationIndex.addLocations(locationMap);

        // Discard all the entry from the write cache, since they're now persisted
        writeCacheBeingFlushed = null;

        double flushTime = (System.nanoTime() - start) / 1e9;
        double flushThroughput = sizeToFlush / 1024 / 1024 / flushTime;
        log.info("Flushing done time {} s -- Written {} Mb/s", flushTime, flushThroughput);
    }

    @Override
    public void trimEntries(long ledgerId, long lastEntryId) throws IOException {
        log.debug("Trim entries {}@{}", ledgerId, lastEntryId);

        // Trimming only affects entries that are still in the write cache
        writeCacheMutex.readLock().lock();
        try {
            NavigableMap<LongPair, ByteBuffer> entriesToDelete = writeCache.subMap(new LongPair(ledgerId, 0), true,
                    new LongPair(ledgerId, lastEntryId), true);

            long deletedSize = 0;
            for (ByteBuffer b : entriesToDelete.values()) {
                deletedSize += b.remaining();
            }

            writeCacheSizeTrimmed.addAndGet(deletedSize);
            entriesToDelete.clear();
        } finally {
            writeCacheMutex.readLock().unlock();
        }

    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        log.debug("Deleting ledger {}", ledgerId);

        // Delete entries from this ledger that are still in the write cache
        writeCacheMutex.readLock().lock();
        try {
            NavigableMap<LongPair, ByteBuffer> entriesToDelete = writeCache.subMap(new LongPair(ledgerId, 0), true,
                    new LongPair(ledgerId + 1, 0), false);

            entriesToDelete.clear();
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        entryLocationIndex.delete(ledgerId);
        ledgerIndex.delete(ledgerId);
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId)
            throws IOException {
        return ledgerIndex.getActiveLedgersInRange(firstLedgerId, lastLedgerId);
    }

    @Override
    public synchronized void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        // Trigger a flush to have all the entries being compacted in the db storage
        flush();

        entryLocationIndex.updateLocations(locations);
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return new BKMBeanInfo() {
            public boolean isHidden() {
                return false;
            }

            public String getName() {
                return "DbLedgerStorage";
            }
        };
    }

    @Override
    public EntryLogger getEntryLogger() {
        return entryLogger;
    }

    /**
     * Add an already existing ledger to the index.
     * 
     * This method is only used as a tool to help the migration from InterleaveLedgerStorage to DbLedgerStorage
     * 
     * @param ledgerId
     *            the ledger id
     * @param entries
     *            a map of entryId -> location
     * @return the number of
     */
    public long addLedgerToIndex(long ledgerId, boolean isFenced, byte[] masterKey,
            Iterable<SortedMap<Long, Long>> entries) throws Exception {
        LedgerData ledgerData = LedgerData.newBuilder().setExists(true).setFenced(isFenced)
                .setMasterKey(ByteString.copyFrom(masterKey)).build();
        ledgerIndex.set(ledgerId, ledgerData);
        long numberOfEntries = 0;

        // Iterate over all the entries pages
        for (SortedMap<Long, Long> page : entries) {
            Multimap<Long, LongPair> locationMap = ArrayListMultimap.create();
            List<LongPair> locations = Lists.newArrayListWithExpectedSize(page.size());
            for (long entryId : page.keySet()) {
                locations.add(new LongPair(entryId, page.get(entryId)));
                ++numberOfEntries;
            }

            locationMap.putAll(ledgerId, locations);
            entryLocationIndex.addLocations(locationMap);
        }

        return numberOfEntries;
    }

    private static final Logger log = LoggerFactory.getLogger(DbLedgerStorage.class);
}
