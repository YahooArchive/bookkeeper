package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * Maintains an index for the ledgers metadata.
 * 
 * The key is the ledgerId and the value is the {@link LedgerData} content.
 */
public class LedgerMetadataIndex implements Closeable {
    // Contains all ledgers stored in the bookie
    private final ConcurrentMap<Long, LedgerData> ledgers;
    private final AtomicInteger ledgersCount;

    private final KeyValueStorage ledgersDb;
    private StatsLogger stats;

    // Holds ledger modifications applied in memory map, and pending to be flushed on db
    private final ConcurrentLinkedQueue<Entry<Long, LedgerData>> pendingLedgersUpdates;

    // Holds ledger ids that were delete from memory map, and pending to be flushed on db
    private final ConcurrentLinkedQueue<Long> pendingDeletedLedgers;

    public LedgerMetadataIndex(String basePath, StatsLogger stats) throws IOException {
        String ledgersPath = FileSystems.getDefault().getPath(basePath, "ledgers").toFile().toString();
        ledgersDb = new KeyValueStorageLevelDB(ledgersPath);

        ledgers = Maps.newConcurrentMap();
        ledgersCount = new AtomicInteger();

        // Read all ledgers from db
        CloseableIterator<Entry<byte[], byte[]>> iterator = ledgersDb.iterator();
        try {
            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();
                long ledgerId = fromArray(entry.getKey());
                LedgerData ledgerData = LedgerData.parseFrom(entry.getValue());
                ledgers.put(ledgerId, ledgerData);
                ledgersCount.incrementAndGet();
            }
        } finally {
            iterator.close();
        }

        this.pendingLedgersUpdates = new ConcurrentLinkedQueue<Entry<Long, LedgerData>>();
        this.pendingDeletedLedgers = new ConcurrentLinkedQueue<Long>();

        this.stats = stats;
        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("ledgers-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return (long) ledgersCount.get();
            }
        });
    }

    @Override
    public void close() throws IOException {
        ledgersDb.close();
    }

    public LedgerData get(long ledgerId) throws IOException {
        LedgerData ledgerData = ledgers.get(ledgerId);
        if (ledgerData == null) {
            log.debug("Ledger not found {}", ledgerId);
            throw new Bookie.NoLedgerException(ledgerId);
        }

        return ledgerData;
    }

    public void set(long ledgerId, LedgerData ledgerData) throws IOException {
        ledgerData = LedgerData.newBuilder(ledgerData).setExists(true).build();

        if (ledgers.put(ledgerId, ledgerData) == null) {
            log.debug("Added new ledger {}", ledgerId);
            ledgersCount.incrementAndGet();
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, ledgerData));
    }

    public void delete(long ledgerId) throws IOException {
        if (ledgers.remove(ledgerId) != null) {
            log.debug("Removed ledger {}", ledgerId);
            ledgersCount.decrementAndGet();
        }

        pendingDeletedLedgers.add(ledgerId);
    }

    public Iterable<Long> getActiveLedgersInRange(final long firstLedgerId, final long lastLedgerId) throws IOException {
        return Iterables.filter(ledgers.keySet(), new Predicate<Long>() {
            @Override
            public boolean apply(Long ledgerId) {
                return ledgerId >= firstLedgerId && ledgerId < lastLedgerId;
            }
        });
    }

    static long fromArray(byte[] array) {
        checkArgument(array.length == 8);
        return ByteBuffer.wrap(array).getLong();
    }

    static byte[] toArray(long n) {
        return ByteBuffer.allocate(8).putLong(n).array();
    }

    public boolean setFenced(long ledgerId) throws IOException {
        LedgerData ledgerData = get(ledgerId);
        if (ledgerData.getFenced()) {
            return false;
        }

        LedgerData newLedgerData = LedgerData.newBuilder(ledgerData).setFenced(true).build();

        if (ledgers.put(ledgerId, newLedgerData) == null) {
            // Ledger had been deleted
            log.debug("Re-inserted fenced ledger {}", ledgerId);
            ledgersCount.incrementAndGet();
        } else {
            log.debug("Set fenced ledger {}", ledgerId);
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, newLedgerData));
        return true;
    }

    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        LedgerData ledgerData = ledgers.get(ledgerId);
        if (ledgerData == null) {
            // New ledger inserted
            ledgerData = LedgerData.newBuilder().setExists(true).setFenced(false)
                    .setMasterKey(ByteString.copyFrom(masterKey)).build();
            log.debug("Inserting new ledger {}", ledgerId);
        } else {
            if (!Arrays.equals(ledgerData.getMasterKey().toByteArray(), masterKey)) {
                log.warn("Ledger {} masterKey in db can only be set once.", ledgerId);
                throw new IOException(BookieException.create(BookieException.Code.IllegalOpException));
            }
        }

        if (ledgers.put(ledgerId, ledgerData) == null) {
            ledgersCount.incrementAndGet();
        }

        pendingLedgersUpdates.add(new SimpleEntry<Long, LedgerData>(ledgerId, ledgerData));
    }

    /**
     * Flushes all pending changes
     */
    public void flush() throws IOException {
        List<Entry<byte[], byte[]>> updates = Lists.newArrayList();

        while (!pendingLedgersUpdates.isEmpty()) {
            Entry<Long, LedgerData> entry = pendingLedgersUpdates.poll();
            byte[] key = toArray(entry.getKey());
            byte[] value = entry.getValue().toByteArray();
            updates.add(new SimpleEntry<byte[], byte[]>(key, value));
        }

        log.debug("Persisting updates to {} ledgers", updates.size());
        ledgersDb.put(updates);

        List<byte[]> deletes = Lists.newArrayList();
        while (!pendingDeletedLedgers.isEmpty()) {
            long ledgerId = pendingDeletedLedgers.poll();
            deletes.add(toArray(ledgerId));
        }

        log.debug("Persisting deletes of ledgers", deletes.size());
        ledgersDb.delete(deletes);
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataIndex.class);
}
