package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.util.internal.ConcurrentSet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.bookie.storage.ldb.SortedLruCache.Weighter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Maintains an index of the entry locations in the EntryLogger.
 * <p>
 * For each ledger multiple entries are stored in the same "record", represented by the {@link LedgerIndexPage} class.
 */
public class EntryLocationIndex implements Closeable {

    static class EntryRange extends LongPair {
        final long lastEntry;

        EntryRange(long ledgerId, long firstEntry, long lastEntry) {
            super(ledgerId, firstEntry);
            this.lastEntry = lastEntry;
            if (log.isDebugEnabled()) {
                log.debug("Created new entry range ({}, {}, {})", new Object[] { ledgerId, firstEntry, lastEntry });
            }
        }

        @Override
        public int compareTo(LongPair lp) {
            if (lp instanceof EntryRange) {
                EntryRange er = (EntryRange) lp;
                if (log.isDebugEnabled()) {
                    log.debug("Comparing range {} with other range {}", this, er);
                }
                return ComparisonChain.start().compare(first, er.first).compare(second, er.second)
                        .compare(lastEntry, er.lastEntry).result();
            } else {
                long otherLedgerId = lp.first;
                long otherEntryId = lp.second;

                if (log.isDebugEnabled()) {
                    log.debug("Comparing range ({}, {}, {}) to entry {}, {}", new Object[] { first, second, lastEntry,
                            otherLedgerId, otherEntryId });
                }
                if (first != otherLedgerId) {
                    return Long.compare(first, otherLedgerId);
                } else {
                    if (otherEntryId < second) {
                        return +1;
                    } else if (otherEntryId > lastEntry) {
                        return -1;
                    } else {
                        return 0;
                    }
                }
            }
        }

        @Override
        public String toString() {
            return String.format("(%d,%d,%d)", first, second, lastEntry);
        }
    }

    private final KeyValueStorage locationsDb;
    private final SortedLruCache<LongPair, LedgerIndexPage> locationsCache;
    private final ConcurrentSet<Long> deletedLedgers = new ConcurrentSet<Long>();

    private StatsLogger stats;

    public EntryLocationIndex(KeyValueStorageFactory storageFactory, String basePath, StatsLogger stats,
            long entryLocationCacheMaxSize) throws IOException {
        String locationsDbPath = FileSystems.getDefault().getPath(basePath, "locations").toFile().toString();
        locationsDb = storageFactory.newKeyValueStorage(locationsDbPath);

        // Convert max memory size to max number of entries
        long maxNumberOfEntries = entryLocationCacheMaxSize / LedgerIndexPage.SIZE_OF_LONG;
        locationsCache = new SortedLruCache<LongPair, LedgerIndexPage>(maxNumberOfEntries,
                new Weighter<LedgerIndexPage>() {
                    public long getSize(LedgerIndexPage ledgerIndexPage) {
                        return ledgerIndexPage.getNumberOfEntries();
                    }
                });

        this.stats = stats;
        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("locations-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return locationsCache.getSize();
            }
        });
        stats.registerGauge("locations-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return locationsCache.getNumberOfEntries();
            }
        });
    }

    @Override
    public void close() throws IOException {
        locationsDb.close();
    }

    public long getLocation(long ledgerId, long entryId) throws IOException {
        return getLedgerIndexPage(ledgerId, entryId).getPosition(entryId);
    }

    LedgerIndexPage getLedgerIndexPage(long ledgerId, long entryId) throws IOException {
        if (deletedLedgers.contains(ledgerId)) {
            if (log.isDebugEnabled()) {
                log.debug("Entry not found {}@{} - Ledger already deleted", ledgerId, entryId);
            }
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }

        LedgerIndexPage ledgerIndexPage = locationsCache.get(new LongPair(ledgerId, entryId));
        if (ledgerIndexPage != null) {
            if (log.isDebugEnabled()) {
                log.debug("Found ledger index page for {}@{} in cache", ledgerId, entryId);
            }
            return ledgerIndexPage;
        }

        if (log.isDebugEnabled()) {
            log.debug("Loading ledger index page for {}@{} from db", ledgerId, entryId);
        }

        LongPair key = new LongPair(ledgerId, entryId + 1);
        Entry<byte[], byte[]> entry = locationsDb.getFloor(key.toArray());
        if (entry == null) {
            if (log.isDebugEnabled()) {
                log.debug("1. Entry not found {}@{}", ledgerId, entryId);
            }
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }

        ledgerIndexPage = new LedgerIndexPage(entry.getKey(), entry.getValue());
        if (ledgerIndexPage.getLedgerId() != ledgerId || entryId < ledgerIndexPage.getFirstEntry()
                || entryId > ledgerIndexPage.getLastEntry()) {
            if (log.isDebugEnabled()) {
                log.debug("2. Entry not found {}@{}", ledgerId, entryId);
                log.debug("Not found.. entries: {}", ledgerIndexPage);
            }
            throw new Bookie.NoEntryException(ledgerId, entryId);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Found page in db: {}", ledgerIndexPage);
            }
            locationsCache.put(new EntryRange(ledgerIndexPage.getLedgerId(), ledgerIndexPage.getFirstEntry(),
                    ledgerIndexPage.getLastEntry()), ledgerIndexPage);
            return ledgerIndexPage;
        }
    }

    public Long getLastEntryInLedger(long ledgerId) throws IOException {
        if (deletedLedgers.contains(ledgerId)) {
            // Ledger already deleted
            return -1L;
        }

        LongPair nextEntryKey = new LongPair(ledgerId + 1, 0);

        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(nextEntryKey.toArray());
        if (entry == null) {
            return -1L;
        }

        LedgerIndexPage ledgerIndexPage = new LedgerIndexPage(entry.getKey(), entry.getValue());
        if (ledgerIndexPage.getLedgerId() != ledgerId) {
            // There is no entry in the ledger we are looking into
            return -1L;
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Found last page in storage db for ledger {} : {}", ledgerId, ledgerIndexPage);
            }
            return ledgerIndexPage.getLastEntry();
        }
    }

    public void addLocations(Multimap<Long, LongPair> locationMap) throws IOException {
        Batch batch = locationsDb.newBatch();

        if (log.isDebugEnabled()) {
            log.debug("Add locations for {} ledgers", locationMap.keySet().size());
        }

        // For each ledger with new entries in the write cache, we write a single record, containing all the
        // offsets for all its own entries
        for (long ledgerId : locationMap.keySet()) {
            final long lastEntryId = getLastEntryInLedger(ledgerId);

            List<LongPair> entries = (List<LongPair>) locationMap.get(ledgerId);
            LedgerIndexPage olderIndexPage = null;

            if (log.isDebugEnabled()) {
                log.debug("Add locations for ledger {} -- locations: {}", ledgerId, entries.size());
            }

            // First check entries that arrived out of order
            int entriesOutOfOrder = 0;
            for (LongPair entry : entries) {
                long entryId = entry.first;
                if (entryId > lastEntryId) {
                    // No more entries out of order
                    if (log.isDebugEnabled()) {
                        log.debug("No more out of order: ledger: {} -- entry: {} -- last-in-ledger: {}", new Object[] {
                                ledgerId, entryId, lastEntryId });
                    }
                    break;
                }

                ++entriesOutOfOrder;

                if (log.isDebugEnabled()) {
                    log.debug("Storing entry out of order: ledger: {} -- entry: {} -- last-in-ledger: {}",
                            new Object[] { ledgerId, entryId, lastEntryId });
                }

                if (olderIndexPage == null || !olderIndexPage.includes(entryId)) {
                    // Find the correct ledger index page to update
                    try {
                        olderIndexPage = getLedgerIndexPage(ledgerId, entryId);
                    } catch (NoEntryException e) {
                        // If we cannot find the index page, we need to create a new one
                        olderIndexPage = new LedgerIndexPage(ledgerId, Lists.newArrayList(entry));
                    }

                    batch.put(olderIndexPage.getKey(), olderIndexPage.getValue());
                }

                olderIndexPage.setPosition(entryId, entry.second);
            }

            if (entriesOutOfOrder > 0) {
                // Remove the entries out of order, since they were already inserted here above
                entries = entries.subList(entriesOutOfOrder, entries.size());
            }

            if (entries.isEmpty()) {
                // All the entries for this ledger were filtered out
                continue;
            }

            LedgerIndexPage indexPage = new LedgerIndexPage(ledgerId, entries);

            if (log.isDebugEnabled()) {
                log.debug("Adding page to index: {}", indexPage);
            }
            batch.put(indexPage.getKey(), indexPage.getValue());
        }

        batch.flush();
    }

    public synchronized void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        Set<LedgerIndexPage> pagesUpdated = Sets.newHashSet();

        if (log.isDebugEnabled()) {
            log.debug("Update locations -- {}", Iterables.size(newLocations));
        }

        // Update all the ledger index pages with the new locations
        for (EntryLocation e : newLocations) {
            if (log.isDebugEnabled()) {
                log.debug("Update location - ledger: {} -- entry: {}", e.ledger, e.entry);
            }
            LedgerIndexPage indexPage = getLedgerIndexPage(e.ledger, e.entry);
            indexPage.setPosition(e.entry, e.location);
            pagesUpdated.add(indexPage);
        }

        if (log.isDebugEnabled()) {
            log.debug("Updated pages -- {}", pagesUpdated.size());
        }

        // Store the pages back in the db
        Batch batch = locationsDb.newBatch();
        for (LedgerIndexPage indexPage : pagesUpdated) {
            batch.put(indexPage.getKey(), indexPage.getValue());
        }

        batch.flush();
    }

    public void delete(long ledgerId) throws IOException {
        // We need to find all the LedgerIndexPage records belonging to one specific ledgers
        deletedLedgers.add(ledgerId);

        LongPair firstKey = new LongPair(ledgerId, 0);
        LongPair lastKey = new LongPair(ledgerId + 1, 0);
        if (log.isDebugEnabled()) {
            log.debug("Deleting from {} to {}", firstKey, lastKey);
        }

        locationsCache.removeRange(firstKey, lastKey);
    }

    public void flush() throws IOException {
        Batch batch = locationsDb.newBatch();

        List<Long> deletedLedgersList = Lists.newArrayList(deletedLedgers);
        for (Long ledgerId : deletedLedgersList) {
            LongPair firstKey = new LongPair(ledgerId, 0);
            LongPair lastKey = new LongPair(ledgerId + 1, 0);
            if (log.isDebugEnabled()) {
                log.debug("Deleting from {} to {}", firstKey, lastKey);
            }

            CloseableIterator<byte[]> iter = locationsDb.keys(firstKey.toArray(), lastKey.toArray());
            try {
                while (iter.hasNext()) {
                    byte[] key = iter.next();
                    if (log.isDebugEnabled()) {
                        log.debug("Deleting ledger index page ({}, {})", LongPair.fromArray(key).first,
                                LongPair.fromArray(key).second);
                    }

                    batch.remove(key);
                }
            } finally {
                iter.close();
            }
        }

        batch.flush();

        // Removed from pending set
        for (Long ledgerId : deletedLedgersList) {
            deletedLedgers.remove(ledgerId);
        }
    }

    void forceCompaction() throws IOException{
       locationsDb.forceCompaction();
    }

    private static final Logger log = LoggerFactory.getLogger(EntryLocationIndex.class);
}
