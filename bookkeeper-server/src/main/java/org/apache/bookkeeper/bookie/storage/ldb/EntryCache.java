package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class EntryCache {
    private final ConcurrentNavigableMap<LongPair, ByteBuf> cache;
    private final ByteBufAllocator allocator;
    private final AtomicLong size;
    private final AtomicLong count;

    public EntryCache(ByteBufAllocator allocator) {
        this.cache = new ConcurrentSkipListMap<LongPair, ByteBuf>();
        this.allocator = allocator;
        this.size = new AtomicLong(0L);
        this.count = new AtomicLong(0L);
    }

    public void put(long ledgerId, long entryId, ByteBuffer entry) {
        ByteBuf buffer = allocator.buffer(entry.remaining(), entry.remaining());
        buffer.writeBytes(entry.duplicate());
        ByteBuf oldValue = cache.put(new LongPair(ledgerId, entryId), buffer);
        if (oldValue != null) {
            size.addAndGet(-oldValue.readableBytes());
            oldValue.release();
        }

        size.addAndGet(buffer.readableBytes());
        count.incrementAndGet();
    }

    public ByteBuffer get(long ledgerId, long entryId) {
        ByteBuf buffer = cache.get(new LongPair(ledgerId, entryId));
        if (buffer == null) {
            return null;
        } else {
            return toByteBuffer(buffer);
        }
    }

    public void invalidate(long ledgerId, long entryId) {
        ByteBuf buf = cache.remove(new LongPair(ledgerId, entryId));
        if (buf != null) {
            size.addAndGet(-buf.readableBytes());
            count.decrementAndGet();
            buf.release();
        }
    }

    public ByteBuffer getLastEntry(long ledgerId) {
        // Next entry key is the first entry of the next ledger, we first seek to that entry and then step back to find
        // the last entry on ledgerId
        LongPair nextEntryKey = new LongPair(ledgerId + 1, 0);

        Entry<LongPair, ByteBuf> mapEntry = cache.headMap(nextEntryKey).lastEntry();
        if (mapEntry != null && mapEntry.getKey().first == ledgerId) {
            return toByteBuffer(mapEntry.getValue());
        } else {
            return null;
        }
    }

    public long deleteLedger(long ledgerId) {
        return deleteEntries(new LongPair(ledgerId, 0), new LongPair(ledgerId + 1, 0), false);
    }

    public long trimLedger(long ledgerId, long lastEntryId) {
        return deleteEntries(new LongPair(ledgerId, 0), new LongPair(ledgerId, lastEntryId), true);
    }

    private long deleteEntries(LongPair start, LongPair end, boolean endIncluded) {
        NavigableMap<LongPair, ByteBuf> entriesToDelete = cache.subMap(start, true, end, endIncluded);

        long deletedSize = 0;
        long deletedCount = 0;
        while (true) {
            Entry<LongPair, ByteBuf> entry = entriesToDelete.pollFirstEntry();
            if (entry == null) {
                break;
            }

            deletedSize += entry.getValue().readableBytes();
            deletedCount++;
            entry.getValue().release();
        }

        size.addAndGet(-deletedSize);
        count.addAndGet(-deletedCount);
        return deletedSize;
    }

    public Set<Entry<LongPair, ByteBuf>> entries() {
        return cache.entrySet();
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public long size() {
        return size.get();
    }

    public long count() {
        return count.get();
    }

    public void clear() {
        while (true) {
            Entry<LongPair, ByteBuf> entry = cache.pollFirstEntry();
            if (entry == null) {
                break;
            }

            ByteBuf content = entry.getValue();
            size.addAndGet(-content.readableBytes());
            content.release();
        }
        count.set(0L);
    }

    private static ByteBuffer toByteBuffer(ByteBuf buf) {
        ByteBuffer buffer = ByteBuffer.allocate(buf.readableBytes());
        buf.duplicate().readBytes(buffer);
        buffer.flip();
        return buffer;
    }
}
