package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectorThread;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;

public class DbLedgerStorage implements CompactableLedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(DbLedgerStorage.class);

    private EntryLogger entryLogger;

    private LedgerMetadataIndex ledgerIndex;
    private EntryLocationIndex entryLocationIndex;

    private GarbageCollectorThread gcThread;

    private final PooledByteBufAllocator allocator = new PooledByteBufAllocator(false);

    // Write cache where all new entries are inserted into
    private EntryCache writeCache = new EntryCache(allocator);

    // Write cache that is used to swap with writeCache during flushes
    private EntryCache writeCacheBeingFlushed = new EntryCache(allocator);

    // Cache where we insert entries for speculative reading
    private final EntryCache readAheadCache = new EntryCache(allocator);

    private final AtomicLong writeCacheSizeTrimmed = new AtomicLong(0);
    private final ReentrantReadWriteLock writeCacheMutex = new ReentrantReadWriteLock();

    private final RateLimiter writeRateLimiter = RateLimiter.create(5000);
    private final RateLimiter readRateLimiter = RateLimiter.create(100);
    private final AtomicBoolean isThrottlingRequests = new AtomicBoolean(false);

    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
            "db-storage-%s").build());

    static final String WRITE_CACHE_MAX_SIZE_MB = "dbStorage_writeCacheMaxSizeMb";
    static final String WRITE_CACHE_CHUNK_SIZE_MB = "dbStorage_writeCacheChunkSizeMb";
    static final String READ_AHEAD_CACHE_BATCH_SIZE = "dbStorage_readAheadCacheBatchSize";
    static final String READ_AHEAD_CACHE_MAX_SIZE_MB = "dbStorage_readAheadCacheMaxSizeMb";
    static final String TRIM_ENABLED = "dbStorage_trimEnabled";

    private static final long DEFAULT_WRITE_CACHE_MAX_SIZE_MB = 16;
    private static final long DEFAULT_READ_AHEAD_CACHE_MAX_SIZE_MB = 16;
    private static final int DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE = 100;
    private static final int MB = 1024 * 1024;

    private long writeCacheMaxSize;

    private CheckpointSource checkpointSource = null;
    private Checkpoint lastCheckpoint = Checkpoint.MIN;

    private long readAheadCacheMaxSize;
    private int readAheadCacheBatchSize;
    private boolean trimEnabled;

    private StatsLogger stats;

    private OpStatsLogger addEntryStats;
    private OpStatsLogger readEntryStats;
    private OpStatsLogger readCacheHitStats;
    private OpStatsLogger readCacheMissStats;
    private OpStatsLogger readAheadBatchCountStats;
    private OpStatsLogger readAheadBatchSizeStats;
    private OpStatsLogger flushStats;
    private OpStatsLogger flushSizeStats;
    private OpStatsLogger trimStats;

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager,
            LedgerDirsManager indexDirsManager, CheckpointSource checkpointSource, StatsLogger statsLogger)
            throws IOException {
        checkArgument(ledgerDirsManager.getAllLedgerDirs().size() == 1,
                "Db implementation only allows for one storage dir");

        String baseDir = ledgerDirsManager.getAllLedgerDirs().get(0).toString();

        writeCacheMaxSize = conf.getLong(WRITE_CACHE_MAX_SIZE_MB, DEFAULT_WRITE_CACHE_MAX_SIZE_MB) * MB;

        this.checkpointSource = checkpointSource;

        readAheadCacheMaxSize = conf.getLong(READ_AHEAD_CACHE_MAX_SIZE_MB, DEFAULT_READ_AHEAD_CACHE_MAX_SIZE_MB) * MB;
        readAheadCacheBatchSize = conf.getInt(READ_AHEAD_CACHE_BATCH_SIZE, DEFAULT_READ_AHEAD_CACHE_BATCH_SIZE);

        trimEnabled = conf.getBoolean(TRIM_ENABLED, false);
        this.stats = statsLogger;

        log.info("Started Db Ledger Storage - Write cache size: {} Mb - Trim enabled: {}",
                writeCacheMaxSize / 1024 / 1024, trimEnabled);

        ledgerIndex = new LedgerMetadataIndex(baseDir, stats);
        entryLocationIndex = new EntryLocationIndex(baseDir, stats);

        entryLogger = new EntryLogger(conf, ledgerDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerManager, this);

        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("write-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCache.size() + writeCacheBeingFlushed.size();
            }
        });
        stats.registerGauge("write-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCache.count() + writeCacheBeingFlushed.count();
            }
        });
        stats.registerGauge("read-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readAheadCache.size();
            }
        });
        stats.registerGauge("read-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return readAheadCache.count();
            }
        });
        stats.registerGauge("trim-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return writeCacheSizeTrimmed.get();
            }
        });

        stats.registerGauge("ledgers-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                try {
                    return ledgerIndex.count();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        addEntryStats = stats.getOpStatsLogger("add-entry");
        readEntryStats = stats.getOpStatsLogger("read-entry");
        readCacheHitStats = stats.getOpStatsLogger("read-cache-hits");
        readCacheMissStats = stats.getOpStatsLogger("read-cache-misses");
        readAheadBatchCountStats = stats.getOpStatsLogger("readahead-batch-count");
        readAheadBatchSizeStats = stats.getOpStatsLogger("readahead-batch-size");
        trimStats = stats.getOpStatsLogger("trim");
        flushStats = stats.getOpStatsLogger("flush");
        flushSizeStats = stats.getOpStatsLogger("flush-size");
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

            writeCache.clear();
            writeCacheBeingFlushed.clear();
            readAheadCache.clear();

            executor.shutdown();

        } catch (IOException e) {
            log.error("Error closing db storage", e);
        }
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        try {
            LedgerData ledgerData = ledgerIndex.get(ledgerId);
            log.debug("Ledger exists. ledger: {} : {}", ledgerId, ledgerData.getExists());
            return ledgerData.getExists();
        } catch (Bookie.NoLedgerException nle) {
            // ledger does not exist
            return false;
        }
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        log.debug("isFenced. ledger: {}", ledgerId);
        return ledgerIndex.get(ledgerId).getFenced();
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        log.debug("Set fenced. ledger: {}", ledgerId);
        return ledgerIndex.setFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        log.debug("Set master key. ledger: {}", ledgerId);
        ledgerIndex.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        log.debug("Read master key. ledger: {}", ledgerId);
        return ledgerIndex.get(ledgerId).getMasterKey().toByteArray();
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long startTime = MathUtils.nowInNano();

        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        log.debug("Add entry. {}@{}", ledgerId, entryId);
        if (isThrottlingRequests.get()) {
            writeRateLimiter.acquire();
        }

        writeCacheMutex.readLock().lock();
        try {
            writeCache.put(ledgerId, entryId, entry);
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        if (writeCache.size() > writeCacheMaxSize && isThrottlingRequests.compareAndSet(false, true)) {
            // Trigger an early flush in background
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        flush();
                    } catch (IOException e) {
                        log.error("Error during flush", e);
                    }
                }
            });
        }

        recordSuccessfulEvent(addEntryStats, startTime);
        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        long startTime = MathUtils.nowInNano();
        log.debug("Get Entry: {}@{}", ledgerId, entryId);
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntry(ledgerId);
        }

        writeCacheMutex.readLock().lock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuffer entry = writeCache.get(ledgerId, entryId);
            if (entry != null) {
                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer
            entry = writeCacheBeingFlushed.get(ledgerId, entryId);
            if (entry != null) {
                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // Try reading from read-ahead cache
        ByteBuffer entry = readAheadCache.get(ledgerId, entryId);
        if (entry != null) {
            recordSuccessfulEvent(readCacheHitStats, startTime);
            recordSuccessfulEvent(readEntryStats, startTime);
            readAheadCache.invalidate(ledgerId, entryId);
            return entry;
        }

        // When reading from db we want to avoid that too many reads to affect the flush time
        if (isThrottlingRequests.get()) {
            readRateLimiter.acquire();
        }

        // Read from main storage
        try {
            LedgerIndexPage ledgerIndexPage = entryLocationIndex.getLedgerIndexPage(ledgerId, entryId);
            long entryLocation = ledgerIndexPage.getPosition(entryId);
            byte[] content = entryLogger.readEntry(ledgerId, entryId, entryLocation);

            // Try to read more entries
            fillReadAheadCache(ledgerIndexPage, ledgerId, entryId + 1);

            recordSuccessfulEvent(readCacheMissStats, startTime);
            recordSuccessfulEvent(readEntryStats, startTime);

            return ByteBuffer.wrap(content);
        } catch (NoEntryException e) {
            recordFailedEvent(readEntryStats, startTime);

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

    private void fillReadAheadCache(LedgerIndexPage ledgerIndexPage, long ledgerId, long entryId) {
        try {
            long lastEntryInPage = ledgerIndexPage.getLastEntry();
            int count = 0;
            long size = 0;

            while (count < readAheadCacheBatchSize && entryId <= lastEntryInPage
                    && readAheadCache.size() < readAheadCacheMaxSize) {
                long entryLocation = ledgerIndexPage.getPosition(entryId);
                if (entryLocation == 0L) {
                    // Skip entry since it's not stored on this bookie
                    entryId++;
                    continue;
                }

                ByteBuffer entry = ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, entryLocation));

                readAheadCache.put(ledgerId, entryId, entry);
                entryId++;
                count++;
                size += entry.remaining();
            }

            readAheadBatchCountStats.registerSuccessfulValue(count);
            readAheadBatchSizeStats.registerSuccessfulValue(size);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Exception during read ahead: {}@{}: e", new Object[] { ledgerId, entryId, e });
            }
        }
    }

    public ByteBuffer getLastEntry(long ledgerId) throws IOException {
        long startTime = MathUtils.nowInNano();

        writeCacheMutex.readLock().lock();
        try {
            // First try to read from the write cache of recent entries
            ByteBuffer entry = writeCache.getLastEntry(ledgerId);
            if (entry != null) {
                if (log.isDebugEnabled()) {
                    long foundLedgerId = entry.getLong(); // ledgedId
                    long entryId = entry.getLong();
                    entry.rewind();
                    log.debug("Found last entry for ledger {} in write cache: {}@{}", new Object[] { ledgerId,
                            foundLedgerId, entryId });
                }

                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }

            // If there's a flush going on, the entry might be in the flush buffer
            entry = writeCacheBeingFlushed.getLastEntry(ledgerId);
            if (entry != null) {
                if (log.isDebugEnabled()) {
                    entry.getLong(); // ledgedId
                    long entryId = entry.getLong();
                    entry.rewind();
                    log.debug("Found last entry for ledger {} in write cache being flushed: {}", ledgerId, entryId);
                }

                recordSuccessfulEvent(readCacheHitStats, startTime);
                recordSuccessfulEvent(readEntryStats, startTime);
                return entry;
            }
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        // Search the last entry in storage
        long lastEntryId = entryLocationIndex.getLastEntryInLedger(ledgerId);
        log.debug("Found last entry for ledger {} in db: {}", ledgerId, lastEntryId);

        long entryLocation = entryLocationIndex.getLocation(ledgerId, lastEntryId);
        byte[] content = entryLogger.readEntry(ledgerId, lastEntryId, entryLocation);

        recordSuccessfulEvent(readCacheMissStats, startTime);
        recordSuccessfulEvent(readEntryStats, startTime);
        return ByteBuffer.wrap(content);
    }

    @VisibleForTesting
    boolean isFlushRequired() {
        writeCacheMutex.readLock().lock();
        try {
            // Even if all the entries have been trimmed, we need to trigger a flush so that we can advance our position
            // in the journal.
            return !writeCache.isEmpty() || writeCacheSizeTrimmed.get() > 0;
        } finally {
            writeCacheMutex.readLock().unlock();
        }
    }

    @Override
    public synchronized Checkpoint checkpoint(Checkpoint checkpoint) throws IOException {
        Checkpoint thisCheckpoint = checkpointSource.newCheckpoint();
        if (lastCheckpoint.compareTo(checkpoint) > 0) {
            return lastCheckpoint;
        }

        long startTime = MathUtils.nowInNano();

        writeCacheMutex.writeLock().lock();

        long sizeTrimmed;
        try {
            // First, swap the current write-cache map with an empty one so that writes will go on unaffected
            // Only a single flush is happening at the same time
            EntryCache tmp = writeCacheBeingFlushed;
            writeCacheBeingFlushed = writeCache;
            writeCache = tmp;

            sizeTrimmed = writeCacheSizeTrimmed.getAndSet(0);

            // Write cache is empty now, so we can accept writes at full speed again
            isThrottlingRequests.set(false);
        } finally {
            writeCacheMutex.writeLock().unlock();
        }

        long sizeToFlush = writeCacheBeingFlushed.size();
        log.debug("Flushing entries. size {} Mb -- Already trimmed: {} Mb", sizeToFlush / 1024.0 / 1024,
                sizeTrimmed / 1024.0 / 1024);

        // Write all the pending entries into the entry logger and collect the offset position for each entry
        Multimap<Long, LongPair> locationMap = ArrayListMultimap.create();
        for (Entry<LongPair, ByteBuf> entry : writeCacheBeingFlushed.entries()) {
            LongPair ledgerAndEntry = entry.getKey();
            ByteBuf content = entry.getValue();
            long ledgerId = ledgerAndEntry.first;
            long entryId = ledgerAndEntry.second;

            long location = entryLogger.addEntry(ledgerId, content.nioBuffer(), true);
            locationMap.put(ledgerId, new LongPair(entryId, location));
        }

        entryLogger.flush();

        entryLocationIndex.addLocations(locationMap);
        lastCheckpoint = thisCheckpoint;

        // Discard all the entry from the write cache, since they're now persisted
        writeCacheBeingFlushed.clear();

        long flushTime = MathUtils.elapsedNanos(startTime);
        double flushThroughput = sizeToFlush / 1024 / 1024 / flushTime / 1e9;
        log.debug("Flushing done time {} s -- Written {} Mb/s", flushTime / 1e9, flushThroughput);

        recordSuccessfulEvent(flushStats, startTime);
        flushSizeStats.registerSuccessfulValue(sizeToFlush);
        return lastCheckpoint;
    }

    @Override
    public synchronized void flush() throws IOException {
        checkpoint(Checkpoint.MAX);
    }

    @Override
    public void trimEntries(long ledgerId, long lastEntryId) throws IOException {
        if (!trimEnabled) {
            return;
        }

        log.debug("Trim entries {}@{}", ledgerId, lastEntryId);
        long startTime = MathUtils.nowInNano();

        // Trimming only affects entries that are still in the write cache
        writeCacheMutex.readLock().lock();
        try {
            long deletedSize = writeCache.trimLedger(ledgerId, lastEntryId);
            writeCacheSizeTrimmed.addAndGet(deletedSize);
            readAheadCache.trimLedger(ledgerId, lastEntryId);

            trimStats.registerSuccessfulEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
        } catch (Exception e) {
            trimStats.registerFailedEvent(MathUtils.elapsedNanos(startTime), TimeUnit.NANOSECONDS);
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
            writeCache.deleteLedger(ledgerId);
        } finally {
            writeCacheMutex.readLock().unlock();
        }

        readAheadCache.deleteLedger(ledgerId);
        entryLocationIndex.delete(ledgerId);
        ledgerIndex.delete(ledgerId);
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        return ledgerIndex.getActiveLedgersInRange(firstLedgerId, lastLedgerId);
    }

    @Override
    public synchronized void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        // Trigger a flush to have all the entries being compacted in the db storage
        flush();

        entryLocationIndex.updateLocations(locations);
    }

    // No mbeans to expose
    public static interface DbLedgerStorageMXBean {
    }

    public static class DbLedgerStorageBean implements DbLedgerStorageMXBean, BKMBeanInfo {
        public boolean isHidden() {
            return true;
        }

        public String getName() {
            return "DbLedgerStorage";
        }
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return new DbLedgerStorageBean();
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

    private void recordSuccessfulEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private void recordFailedEvent(OpStatsLogger logger, long startTimeNanos) {
        logger.registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos), TimeUnit.NANOSECONDS);
    }

    private static final Logger log = LoggerFactory.getLogger(DbLedgerStorage.class);
}
