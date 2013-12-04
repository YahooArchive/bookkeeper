package org.apache.bookkeeper.bookie.storage.ldb;

import java.nio.ByteBuffer;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Queues;

public class WriteCache {

    final static int ChunkSize = 128 * 1024 * 1024;

    private final Queue<ByteBuffer> emptyBuffers = Queues.newArrayDeque();
    private final Queue<ByteBuffer> fullBuffers = Queues.newArrayDeque();
    private final Queue<ByteBuffer> beingFlushedBuffers = Queues.newArrayDeque();

    private ByteBuffer buffer;

    public WriteCache() {
        buffer = ByteBuffer.allocate(ChunkSize);
    }

    public ByteBuffer addEntry(ByteBuffer entry) {
        int position;
        ByteBuffer cachedEntry;

        synchronized (this) {
            if (buffer.remaining() < entry.remaining()) {
                // Get a new memory chunk
                fullBuffers.add(buffer);
                buffer = getBuffer();
            }

            position = buffer.position();
            cachedEntry = buffer.duplicate();
            buffer.position(buffer.position() + entry.remaining());
        }

        // Copy the entry content into the write cache buffer without holding the mutex
        cachedEntry.put(entry.duplicate());
        cachedEntry.position(position);
        cachedEntry.limit(position + entry.remaining());
        return cachedEntry;
    }

    public synchronized void startFlush() {
        beingFlushedBuffers.addAll(fullBuffers);
        beingFlushedBuffers.add(buffer);
        fullBuffers.clear();
        buffer = getBuffer();
    }

    public synchronized void doneFlush() {
        emptyBuffers.addAll(beingFlushedBuffers);
        beingFlushedBuffers.clear();
    }

    private ByteBuffer getBuffer() {
        ByteBuffer b = emptyBuffers.poll();
        if (b != null) {
            b.clear();
            return b;
        } else {
            log.info("Allocate new memory chunk --  buffers - full: {} - progress: {}", fullBuffers.size(),
                    beingFlushedBuffers.size());
            return ByteBuffer.allocate(ChunkSize);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(WriteCache.class);
}
