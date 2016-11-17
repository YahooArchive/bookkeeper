package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;

/**
 * Maintains an index of the entry locations in the EntryLogger.
 * <p>
 * For each ledger multiple entries are stored in the same "record", represented by the {@link LedgerIndexPage} class.
 */
public class EntryLocationIndex implements Closeable {

    private final KeyValueStorage locationsDb;
    private final ConcurrentLongHashSet deletedLedgers = new ConcurrentLongHashSet();

    private StatsLogger stats;

    public EntryLocationIndex(ServerConfiguration conf, KeyValueStorageFactory storageFactory, String basePath,
            StatsLogger stats, long entryLocationCacheMaxSize) throws IOException {
        String locationsDbPath = FileSystems.getDefault().getPath(basePath, "locations").toFile().toString();
        convertIfNeeded(locationsDbPath, conf);
        locationsDb = storageFactory.newKeyValueStorage(locationsDbPath, DbConfigType.Huge, conf);
        doCreateMarkerFile(locationsDbPath);

        this.stats = stats;
        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("entries-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                try {
                    return locationsDb.count();
                } catch (IOException e) {
                    return -1L;
                }
            }
        });
    }

    @Override
    public void close() throws IOException {
        locationsDb.close();
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongWrapper value = LongWrapper.get();

        try {
            if (locationsDb.get(key.array, value.array) < 0) {
                if (log.isDebugEnabled()) {
                    log.debug("Entry not found {}@{} in db index", ledgerId, entryId);
                }
                return 0;
            }

            return value.getValue();
        } finally {
            key.recycle();
            value.recycle();
        }
    }
    
    public List<Long> getLocations(long ledgerId, long startEntryId, int count) throws IOException {
    	LongPairWrapper key = LongPairWrapper.get(ledgerId, startEntryId);
    	CloseableIterator<Entry<byte[], byte[]>> it = null;
    	try {
    		List<Long> result = new ArrayList<Long>(count);
    		it = locationsDb.iterator(key.array, true);
    		while (it.hasNext()) {
    			Entry<byte[], byte[]> entry = it.next();
    			if (!Arrays.equals(key.array, entry.getKey())) {
    				break;
    			}
    			result.add(ArrayUtil.getLong(entry.getValue(), 0));
    			if (--count > 0) {
    				key.set(ledgerId, ++startEntryId);
    			} else {
    				break;
    			}
    		}
    		
    		return result;
        } finally {
            key.recycle();
            if (it != null) {
            	it.close();
            }
        }
    }

    public long getLastEntryInLedger(long ledgerId) throws IOException {
        if (deletedLedgers.contains(ledgerId)) {
            // Ledger already deleted
            return -1;
        }

        return getLastEntryInLedgerInternal(ledgerId);
    }

    private long getLastEntryInLedgerInternal(long ledgerId) throws IOException {
        LongPairWrapper maxEntryId = LongPairWrapper.get(ledgerId, Long.MAX_VALUE);

        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(maxEntryId.array);
        maxEntryId.recycle();

        if (entry == null) {
            throw new Bookie.NoEntryException(ledgerId, -1);
        } else {
            long foundLedgerId = ArrayUtil.getLong(entry.getKey(), 0);
            long lastEntryId = ArrayUtil.getLong(entry.getKey(), 8);

            if (foundLedgerId == ledgerId) {
                if (log.isDebugEnabled()) {
                    log.debug("Found last page in storage db for ledger {} - last entry: {}",
                            new Object[] { ledgerId, lastEntryId });
                }
                return lastEntryId;
            } else {
                throw new Bookie.NoEntryException(ledgerId, -1);
            }
        }
    }

    public void addLocation(long ledgerId, long entryId, long location) throws IOException {
        Batch batch = locationsDb.newBatch();
        addLocation(batch, ledgerId, entryId, location);
        batch.flush();
        batch.close();
    }

    public Batch newBatch() {
        return locationsDb.newBatch();
    }

    public void addLocation(Batch batch, long ledgerId, long entryId, long location) throws IOException {
        LongPairWrapper key = LongPairWrapper.get(ledgerId, entryId);
        LongWrapper value = LongWrapper.get(location);

        if (log.isDebugEnabled()) {
            log.debug("Add location - ledger: {} -- entry: {} -- location: {}",
                    new Object[] { ledgerId, entryId, location });
        }

        try {
            batch.put(key.array, value.array);
        } finally {
            key.recycle();
            value.recycle();
        }
    }

    public void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("Update locations -- {}", Iterables.size(newLocations));
        }

        Batch batch = newBatch();
        // Update all the ledger index pages with the new locations
        for (EntryLocation e : newLocations) {
            if (log.isDebugEnabled()) {
                log.debug("Update location - ledger: {} -- entry: {}", e.ledger, e.entry);
            }

            addLocation(batch, e.ledger, e.entry, e.location);
        }

        batch.flush();
        batch.close();
    }

    public void delete(long ledgerId) throws IOException {
        // We need to find all the LedgerIndexPage records belonging to one specific ledgers
        deletedLedgers.add(ledgerId);
    }

    public void removeOffsetFromDeletedLedgers() throws IOException {
        LongPairWrapper firstKeyWrapper = LongPairWrapper.get(-1, -1);
        LongPairWrapper lastKeyWrapper = LongPairWrapper.get(-1, -1);
        LongPairWrapper keyToDelete = LongPairWrapper.get(-1, -1);

        Set<Long> ledgersToDelete = deletedLedgers.items();

        if (ledgersToDelete.isEmpty()) {
            return;
        }

        long startTime = System.nanoTime();
        long deletedCount = 0;
        log.info("Deleting indexes for {} ledgers", ledgersToDelete);
        Batch batch = locationsDb.newBatch();

        try {
            for (long ledgerId : ledgersToDelete) {
                if (log.isDebugEnabled()) {
                    log.debug("Deleting indexes from ledger {}", ledgerId);
                }

                firstKeyWrapper.set(ledgerId, 0);
                lastKeyWrapper.set(ledgerId, Long.MAX_VALUE);

                Entry<byte[], byte[]> firstKeyRes = locationsDb.getCeil(firstKeyWrapper.array);
                if (firstKeyRes == null || ArrayUtil.getLong(firstKeyRes.getKey(), 0) != ledgerId) {
                    // No entries found for ledger
                    log.info("No entries found for ledger {}", ledgerId);
                    continue;
                }

                long firstEntryId = ArrayUtil.getLong(firstKeyRes.getKey(), 8);
                long lastEntryId = getLastEntryInLedgerInternal(ledgerId);
                log.info("Deleting index for ledger {} entries ({} -> {})",
                        new Object[] { ledgerId, firstEntryId, lastEntryId });

                // Iterate over all the keys and remove each of them
                for (long entryId = firstEntryId; entryId <= lastEntryId; entryId++) {
                    keyToDelete.set(ledgerId, entryId);
                    if (log.isDebugEnabled()) {
                        log.debug("Deleting index for ({}, {})", keyToDelete.getFirst(), keyToDelete.getSecond());
                    }

                    batch.remove(keyToDelete.array);
                    ++deletedCount;
                }

                batch.flush();
                batch.clear();
            }
        } finally {
            firstKeyWrapper.recycle();
            lastKeyWrapper.recycle();
            keyToDelete.recycle();
            batch.close();
        }

        log.info("Deleted indexes for {} entries in {} seconds", deletedCount,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0);

        // Removed from pending set
        for (long ledgerId : ledgersToDelete) {
            deletedLedgers.remove(ledgerId);
        }
    }

    ///

    private static final String NEW_FORMAT_MARKER = "single-location-record";

    private void convertIfNeeded(String path, ServerConfiguration conf) throws IOException {
        FileSystem fileSystem = FileSystems.getDefault();
        final Path newFormatMarkerFile = fileSystem.getPath(path, NEW_FORMAT_MARKER);

        if (!Files.exists(fileSystem.getPath(path))) {
            // Database not existing, no need to convert
            return;
        } else if (Files.exists(newFormatMarkerFile)) {
            // Database was already created with new format, no conversion needed
            return;
        }

        // Do conversion from pages of offsets to single record
        log.info("Converting index format to single location records: {}", path);
        long startTime = System.nanoTime();

        KeyValueStorage source = new KeyValueStorageRocksDB(path, DbConfigType.Huge, conf, true /* read-only */);

        long recordsToConvert = source.count();
        log.info("Opened existing db, starting conversion of {} records", recordsToConvert);

        String targetDbPath = path + ".updated";
        KeyValueStorage target = new KeyValueStorageRocksDB(targetDbPath, DbConfigType.Huge, conf);

        double convertedRecords = 0;
        long targetDbRecords = 0;

        LongPairWrapper key = LongPairWrapper.get(0, 0);
        LongWrapper value = LongWrapper.get();

        final int progressIntervalPercent = 10; // update progress at every 10%
        double nextUpdateAtPercent = progressIntervalPercent; // start updating at 10% completion

        // Copy into new database. Write in batches to speed up the insertion
        CloseableIterator<Entry<byte[], byte[]>> iterator = source.iterator();
        try {
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();

                // Add all the entries on the page
                long ledgerId = ArrayUtil.getLong(entry.getKey(), 0);
                long firstEntryId = ArrayUtil.getLong(entry.getKey(), 8);

                byte[] page = entry.getValue();
                for (int i = 0; i < page.length; i += 8) {
                    long location = ArrayUtil.getLong(page, i);

                    if (location != 0) {
                        key.set(ledgerId, firstEntryId + (i / 8));
                        value.set(location);
                        target.put(key.array, value.array);
                        ++targetDbRecords;
                    }
                }

                ++convertedRecords;
                if (recordsToConvert > 0 && convertedRecords / recordsToConvert >= nextUpdateAtPercent / 100) {
                    // Report progress at 10 percent intervals
                    log.info("Updated records {}/{}   {} %", new Object[] { convertedRecords, recordsToConvert,
                            100.0 * convertedRecords / recordsToConvert });
                    nextUpdateAtPercent += progressIntervalPercent;
                }
            }

        } finally {
            iterator.close();
            source.close();

            target.sync();
            target.close();
            key.recycle();
            value.recycle();
        }

        FileUtils.deleteDirectory(new File(path));
        Files.move(fileSystem.getPath(targetDbPath), fileSystem.getPath(path));

        // Create the marked to avoid conversion next time
        Files.createFile(newFormatMarkerFile);

        log.info("Database update done. Total time: {} -- Target db records: {}",
                DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)),
                targetDbRecords);
    }

    static void doCreateMarkerFile(String path) throws IOException {
        try {
            Files.createFile(FileSystems.getDefault().getPath(path, NEW_FORMAT_MARKER));
        } catch (FileAlreadyExistsException e) {
            // Ignore
        }
    }

    private static final Logger log = LoggerFactory.getLogger(EntryLocationIndex.class);
}
