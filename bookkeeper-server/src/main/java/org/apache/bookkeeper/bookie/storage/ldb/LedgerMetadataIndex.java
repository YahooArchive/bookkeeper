package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorageDataFormats.LedgerData;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.CloseableIterator;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * Maintains an index for the ledgers metadata.
 * 
 * The key is the ledgerId and the value is the {@link LedgerData} content.
 */
public class LedgerMetadataIndex implements Closeable {
    // Cache of ledger data, used to avoid deserializing from db entries many times
    private final LoadingCache<Long, LedgerData> ledgersCache;

    private final KeyValueStorage ledgersDb;
    private StatsLogger stats;
    private final ConcurrentHashMap<Long, ReentrantLock> dbWriteLocks = new ConcurrentHashMap<Long, ReentrantLock>();
    private final static int DB_LOCK_BUCKETS = 16;

    public LedgerMetadataIndex(String basePath, StatsLogger stats) throws IOException {
        String ledgersPath = FileSystems.getDefault().getPath(basePath, "ledgers").toFile().toString();
        ledgersDb = new KeyValueStorageLevelDB(ledgersPath);

        ledgersCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES)
                .build(new CacheLoader<Long, LedgerData>() {
                    @Override
                    public LedgerData load(Long ledgerId) throws Exception {
                        log.debug("Loading ledger data from db. ledger {}", ledgerId);
                        byte[] ledgerKey = toArray(ledgerId);
                        byte[] result = ledgersDb.get(ledgerKey);
                        if (result != null) {
                            log.debug("Found in db. ledger {}", ledgerId);
                            return LedgerData.parseFrom(result);
                        } else {
                            // No ledger was found on the db, throws an exception
                            log.debug("Not Found in db. ledger {}", ledgerId);
                            throw new Bookie.NoLedgerException(ledgerId);
                        }
                    }
                });

        for (long i = 0; i < DB_LOCK_BUCKETS; i++) {
            dbWriteLocks.put(i, new ReentrantLock());
        }

        this.stats = stats;
        registerStats();
    }

    public void registerStats() {
        stats.registerGauge("ledgers-cache-size", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return ledgersCache.size();
            }
        });
        stats.registerGauge("ledgers-cache-count", new Gauge<Long>() {
            @Override
            public Long getDefaultValue() {
                return 0L;
            }

            @Override
            public Long getSample() {
                return ledgersCache.stats().loadCount();
            }
        });
        stats.registerGauge("ledgers-cache-hit-rate", new Gauge<Double>() {
            @Override
            public Double getDefaultValue() {
                return 0.0D;
            }

            @Override
            public Double getSample() {
                return ledgersCache.stats().hitRate() * 100;
            }
        });
    }

    @Override
    public void close() throws IOException {
        ledgersCache.invalidateAll();
        ledgersDb.close();
    }

    public long count() throws IOException {
        return ledgersDb.count();
    }

    public LedgerData get(long ledgerId) throws IOException {
        try {
            return ledgersCache.get(ledgerId);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Bookie.NoLedgerException) {
                throw (Bookie.NoLedgerException) (e.getCause());
            }
            throw new IOException(e.getCause());
        }
    }

    public void set(long ledgerId, LedgerData ledgerData) throws IOException {
        ledgerData = LedgerData.newBuilder(ledgerData).setExists(true).build();
        dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).lock();
        try {
            ledgersDb.put(toArray(ledgerId), ledgerData.toByteArray());
            ledgersCache.invalidate(ledgerId);
        } finally {
            dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).unlock();
        }
    }

    public void delete(long ledgerId) throws IOException {
        dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).lock();
        try {
            ledgersDb.delete(toArray(ledgerId));
            ledgersCache.invalidate(ledgerId);
        } finally {
            dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).unlock();
        }
    }

    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) throws IOException {
        List<Long> ledgerIds = Lists.newArrayList();
        CloseableIterator<byte[]> iter = ledgersDb.keys(toArray(firstLedgerId), toArray(lastLedgerId));
        try {
            while (iter.hasNext()) {
                ledgerIds.add(fromArray(iter.next()));
            }
        } finally {
            iter.close();
        }

        return ledgerIds;
    }

    static long fromArray(byte[] array) {
        checkArgument(array.length == 8);
        return ByteBuffer.wrap(array).getLong();
    }

    static byte[] toArray(long n) {
        return ByteBuffer.allocate(8).putLong(n).array();
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerMetadataIndex.class);

    public boolean setFenced(final long ledgerId) throws IOException {
        dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).lock();
        try {
            LedgerData curData = get(ledgerId);
            if (curData.getFenced()) {
                return false;
            }
            curData = LedgerData.newBuilder(curData).setFenced(true).build();
            ledgersDb.put(toArray(ledgerId), curData.toByteArray());
            ledgersCache.invalidate(ledgerId);
        } finally {
            dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).unlock();
        }

        return true;
    }

    public void setMasterKey(final long ledgerId, final byte[] masterKey) throws IOException {
        dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).lock();
        try {
            try {
                LedgerData curData = ledgersCache.get(ledgerId);
                if (!Arrays.equals(curData.getMasterKey().toByteArray(), masterKey)) {
                    log.debug("Ledger {} masterKey in db can only be set once.", ledgerId);
                    throw BookieException.create(BookieException.Code.IllegalOpException);
                }
            } catch (ExecutionException ee) {
                if (ee.getCause() instanceof Bookie.NoLedgerException) {
                    // need to insert new entry in ledgersDb
                    LedgerData newData = LedgerData.newBuilder().setExists(true).setFenced(false)
                            .setMasterKey(ByteString.copyFrom(masterKey)).build();
                    ledgersDb.put(toArray(ledgerId), newData.toByteArray());
                    return;
                }
                log.error("Set masterKey failed for Ledger {} with error: {}", ledgerId, ee.getCause().getMessage());
                throw new IOException(ee.getCause());
            } catch (Exception e) {
                log.error("Set masterKey failed for ledger {} with error: {}", ledgerId, e.getMessage());
                throw new IOException(e);
            }
        } finally {
            dbWriteLocks.get(ledgerId % DB_LOCK_BUCKETS).unlock();
        }
    }

}
