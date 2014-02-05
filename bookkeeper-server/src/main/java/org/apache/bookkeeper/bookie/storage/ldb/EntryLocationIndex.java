package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage.EntryLocation;
import org.apache.bookkeeper.bookie.storage.ldb.SortedLruCache.Weighter;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;
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
            log.debug("Created new entry range ({}, {}, {})", new Object[] { ledgerId, firstEntry, lastEntry });
        }

        @Override
        public int compareTo(LongPair lp) {
            if (lp instanceof EntryRange) {
                EntryRange er = (EntryRange) lp;
                log.debug("Comparing range {} with other range {}", this, er);
                return ComparisonChain.start().compare(first, er.first).compare(second, er.second)
                        .compare(lastEntry, er.lastEntry).result();
            } else {
                long otherLedgerId = lp.first;
                long otherEntryId = lp.second;

                log.debug("Comparing range ({}, {}, {}) to entry {}, {}", new Object[] { first, second, lastEntry,
                        otherLedgerId, otherEntryId });
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

    public EntryLocationIndex(String basePath) throws IOException {
        String locationsDbPath = FileSystems.getDefault().getPath(basePath, "locations").toFile().toString();
        locationsDb = new KeyValueStorageLevelDB(locationsDbPath);

        locationsCache = new SortedLruCache<LongPair, LedgerIndexPage>((long) 1e6, new Weighter<LedgerIndexPage>() {
            public long getSize(LedgerIndexPage ledgerIndexPage) {
                return ledgerIndexPage.getNumberOfEntries();
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

    private LedgerIndexPage getLedgerIndexPage(long ledgerId, long entryId) throws IOException {
        LedgerIndexPage ledgerIndexPage = locationsCache.get(new LongPair(ledgerId, entryId));
        if (ledgerIndexPage != null) {
            log.debug("Found ledger index page for {}@{} in cache", ledgerId, entryId);
            return ledgerIndexPage;
        }

        log.debug("Loading ledger index page for {}@{} from db", ledgerId, entryId);

        LongPair key = new LongPair(ledgerId, entryId + 1);
        Entry<byte[], byte[]> entry = locationsDb.getFloor(key.toArray());
        if (entry == null) {
            log.debug("1. Entry not found {}@{}", ledgerId, entryId);
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }

        ledgerIndexPage = new LedgerIndexPage(entry.getKey(), entry.getValue());
        if (ledgerIndexPage.getLedgerId() != ledgerId || entryId < ledgerIndexPage.getFirstEntry()
                || entryId > ledgerIndexPage.getLastEntry()) {
            log.debug("2. Entry not found {}@{}", ledgerId, entryId);
            log.debug("Not found.. entries: {}", ledgerIndexPage);
            throw new Bookie.NoEntryException(ledgerId, entryId);
        } else {
            log.debug("Found page in db: {}", ledgerIndexPage);
            locationsCache.put(new EntryRange(ledgerIndexPage.getLedgerId(), ledgerIndexPage.getFirstEntry(),
                    ledgerIndexPage.getLastEntry()), ledgerIndexPage);
            return ledgerIndexPage;
        }
    }

    public Long getLastEntryInLedger(long ledgerId) throws IOException {
        LongPair nextEntryKey = new LongPair(ledgerId + 1, 0);

        // Search the last entry in storage
        Entry<byte[], byte[]> entry = locationsDb.getFloor(nextEntryKey.toArray());
        if (entry == null) {
            throw new Bookie.NoEntryException(ledgerId, 0);
        }

        LedgerIndexPage ledgerIndexPage = new LedgerIndexPage(entry.getKey(), entry.getValue());
        if (ledgerIndexPage.getLedgerId() != ledgerId) {
            // There is no entry in the ledger we are looking into
            throw new Bookie.NoEntryException(ledgerId, 0);
        } else {
            return ledgerIndexPage.getLastEntry();
        }
    }

    public void addLocations(Multimap<Long, LongPair> locationMap) throws IOException {
        List<Entry<byte[], byte[]>> batch = Lists.newArrayListWithExpectedSize(locationMap.keys().size());

        // For each ledger with new entries in the write cache, we write a single record, containing all the
        // offsets for all its own entries
        for (long ledgerId : locationMap.keySet()) {
            List<LongPair> entries = (List<LongPair>) locationMap.get(ledgerId);
            LedgerIndexPage indexPage = new LedgerIndexPage(ledgerId, entries);

            log.debug("Adding page to index: {}", indexPage);
            batch.add(indexPage);
        }

        locationsDb.put(batch);
    }

    public synchronized void updateLocations(Iterable<EntryLocation> newLocations) throws IOException {
        Set<LedgerIndexPage> pagesUpdated = Sets.newHashSet();

        // Update all the ledger index pages with the new locations
        for (EntryLocation e : newLocations) {
            LedgerIndexPage indexPage = getLedgerIndexPage(e.ledger, e.entry);
            indexPage.setPosition(e.entry, e.location);
            pagesUpdated.add(indexPage);
        }

        // Store the pages back in the db
        List<Entry<byte[], byte[]>> batch = Lists.newArrayListWithCapacity(pagesUpdated.size());
        for (LedgerIndexPage indexPage : pagesUpdated) {
            batch.add(indexPage);
        }

        locationsDb.put(batch);
    }

    public void delete(long ledgerId) throws IOException {
        // We need to find all the LedgerIndexPage records belonging to one specific ledgers

        List<byte[]> keys = Lists.newArrayList();

        LongPair firstKey = new LongPair(ledgerId, 0);
        LongPair lastKey = new LongPair(ledgerId + 1, 0);
        log.debug("Deleting from {} to {}", firstKey, lastKey);

        CloseableIterator<byte[]> iter = locationsDb.keys(firstKey.toArray(), lastKey.toArray());
        try {
            while (iter.hasNext()) {
                byte[] key = iter.next();
                if (log.isDebugEnabled()) {
                    log.debug("Deleting ledger index page ({}, {})", LongPair.fromArray(key).first,
                              LongPair.fromArray(key).second);
                }

                keys.add(key);
            }
        } finally {
            iter.close();
        }

        locationsDb.delete(keys);
        locationsCache.removeRange(firstKey, lastKey);
    }

    private static final Logger log = LoggerFactory.getLogger(EntryLocationIndex.class);
}
