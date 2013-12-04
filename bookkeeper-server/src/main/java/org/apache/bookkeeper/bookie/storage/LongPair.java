package org.apache.bookkeeper.bookie.storage;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.storage.EntryLocationIndex.EntryRange;

class LongPair implements Comparable<LongPair> {
    final long first;
    final long second;

    public LongPair(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public int compareTo(LongPair o) {
        if (o instanceof EntryRange) {
            return -(o.compareTo(this));
        } else {
            int res = Long.compare(this.first, o.first);
            if (res == 0) {
                return Long.compare(this.second, o.second);
            } else {
                return res;
            }
        }
    }

    public static LongPair fromArray(byte[] array) {
        ByteBuffer buf = ByteBuffer.wrap(array);
        long first = buf.getLong();
        long second = buf.getLong();
        return new LongPair(first, second);
    }

    public byte[] toArray() {
        ByteBuffer buf = ByteBuffer.allocate(2 * 8);
        buf.putLong(first);
        buf.putLong(second);
        buf.flip();
        return buf.array();
    }

    @Override
    public String toString() {
        return String.format("(%d, %d)", first, second);
    }
}