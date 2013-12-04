package org.apache.bookkeeper.bookie.storage;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

/**
 * Maintains an index for the ledgers metadata.
 * 
 * The key is the ledgerId and the value is the {@link LedgerData} content.
 */
public class LedgerMetadataIndex implements Closeable {
    // Cache of ledger data, used to avoid deserializing from db entries many times
    private final LoadingCache<Long, LedgerData> ledgersCache;

    private final KeyValueStorage ledgersDb;

    public LedgerMetadataIndex(String basePath) throws IOException {
        String ledgersPath = FileSystems.getDefault().getPath(basePath, "ledgers").toFile().toString();
        ledgersDb = new KeyValueStorageLevelDB(ledgersPath);

        ledgersCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES)
                .build(new CacheLoader<Long, LedgerData>() {
                    public LedgerData load(Long ledgerId) throws Exception {
                        log.debug("Loading ledger data from db. ledger {}", ledgerId);
                        byte[] ledgerKey = toArray(ledgerId);
                        byte[] result = ledgersDb.get(ledgerKey);
                        if (result != null) {
                            log.debug("Found in db. ledger {}", ledgerId);
                            return LedgerData.fromArray(result);
                        } else {
                            // No ledger was found on the db, return a dummy LedgerData marked as non-existent
                            log.debug("Not Found in db. ledger {}", ledgerId);
                            return new LedgerData(false, false, new byte[0]);
                        }
                    }
                });
    }

    @Override
    public void close() throws IOException {
        ledgersCache.invalidateAll();
        ledgersDb.close();
    }

    public LedgerData get(long ledgerId) throws IOException {
        try {
            return ledgersCache.get(ledgerId);
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
    }

    public void set(long ledgerId, LedgerData ledgerData) throws IOException {
        ledgerData.setExists(true);
        ledgersDb.put(toArray(ledgerId), ledgerData.toArray());
        ledgersCache.invalidate(ledgerId);
    }

    public void delete(long ledgerId) throws IOException {
        ledgersDb.delete(toArray(ledgerId));
        ledgersCache.invalidate(ledgerId);
    }

    public void deleteAll() throws IOException {
        ledgersDb.delete(ledgersDb.keys());
        ledgersCache.invalidateAll();
    }

    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
        List<Long> ledgerIds = Lists.newArrayList();
        for (byte[] key : ledgersDb.keys(toArray(firstLedgerId), toArray(lastLedgerId))) {
            ledgerIds.add(fromArray(key));
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
}
