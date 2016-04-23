package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;

/**
 * The LedgerIndexPage holds the position for a set of entries in a ledger.
 *
 * The position is a Long integer that identifies the EntryLogger and the offset at which the entry data is stored. If
 * the offset in the table is 0, it means that we don't have this entry stored.
 *
 */
public class LedgerIndexPage implements Entry<byte[], byte[]> {
    private final long ledgerId;
    private final long firstEntry;
    private final long lastEntry;
    private final ByteBuffer locationsTable;

    static final int SIZE_OF_LONG = 8;

    public LedgerIndexPage(long ledgerId, List<LongPair> entries) {
        checkArgument(entries.size() > 0);

        this.ledgerId = ledgerId;
        this.firstEntry = entries.get(0).first;
        this.lastEntry = entries.get(entries.size() - 1).first;

        locationsTable = ByteBuffer.allocate(SIZE_OF_LONG * (int) (lastEntry - firstEntry + 1));
        for (LongPair entry : entries) {
            long entryId = entry.first;
            long location = entry.second;

            try {
                checkArgument(entryId >= firstEntry && entryId <= lastEntry);

                int offset = (int) (entryId - firstEntry) * SIZE_OF_LONG;

                locationsTable.putLong(offset, location);
            } catch (Exception e) {
                log.error("Error adding location: ledger: {} firstEntry: {} lastEntry: {} entryId: {} location: {}",
                        new Object[] { ledgerId, firstEntry, lastEntry, entryId, location, e});

                throw new RuntimeException(e);
            }
        }
    }

    public LedgerIndexPage(byte[] key, byte[] content) {
        checkArgument(key.length == 2 * SIZE_OF_LONG);
        checkArgument(content.length >= SIZE_OF_LONG);
        checkArgument(content.length % SIZE_OF_LONG == 0);

        this.locationsTable = ByteBuffer.wrap(content);

        // Read key
        ByteBuffer buf = ByteBuffer.wrap(key);
        this.ledgerId = buf.getLong();
        this.firstEntry = buf.getLong();
        this.lastEntry = (content.length / SIZE_OF_LONG) - 1 + firstEntry;
    }

    public long getPosition(long entryId) {
        checkArgument(entryId >= firstEntry);
        checkArgument(entryId <= lastEntry);

        int offset = (int) (entryId - firstEntry) * SIZE_OF_LONG;
        return locationsTable.getLong(offset);
    }

    public void setPosition(long entryId, long position) {
        checkArgument(entryId >= firstEntry);
        checkArgument(entryId <= lastEntry);

        int offset = (int) (entryId - firstEntry) * SIZE_OF_LONG;
        locationsTable.putLong(offset, position);
    }

    public boolean includes(long entryId) {
        return entryId >= firstEntry && entryId <= lastEntry;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getFirstEntry() {
        return firstEntry;
    }

    public long getLastEntry() {
        for (long last = lastEntry; last >= 0; last--) {
            int offset = (int) (last - firstEntry) * SIZE_OF_LONG;
            if (locationsTable.getLong(offset) != 0) {
                return last;
            }
        }

        // It should never happen, because there will always be at least 1 entry in the page or the page would have
        // never been written (and entries cannot be deleted). Anyway, returning -1 would mean the entry was found.
        return -1;
    }

    public int getNumberOfEntries() {
        return (int) (getLastEntry() - firstEntry + 1);
    }

    @Override
    public byte[] getKey() {
        return getKey(ledgerId, firstEntry);
    }

    @Override
    public byte[] getValue() {
        return locationsTable.array();
    }

    @Override
    public byte[] setValue(byte[] value) {
        throw new UnsupportedOperationException();
    }

    public static byte[] getKey(long ledgerId, long firstEntry) {
        ByteBuffer buffer = ByteBuffer.allocate(2 * SIZE_OF_LONG);
        buffer.putLong(ledgerId);
        buffer.putLong(firstEntry);

        return buffer.array();
    }

    @Override
    public String toString() {
        return String.format("(%d, %d, %d)", ledgerId, firstEntry, lastEntry);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof LedgerIndexPage) {
            LedgerIndexPage o = (LedgerIndexPage) obj;
            return ledgerId == o.ledgerId && firstEntry == o.firstEntry && lastEntry == o.lastEntry;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ledgerId, firstEntry, lastEntry);
    }

    private static final Logger log = LoggerFactory.getLogger(LedgerIndexPage.class);
}
