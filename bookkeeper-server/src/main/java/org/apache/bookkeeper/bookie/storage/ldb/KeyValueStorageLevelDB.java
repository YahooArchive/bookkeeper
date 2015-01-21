package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

import com.google.common.primitives.UnsignedBytes;

public class KeyValueStorageLevelDB implements KeyValueStorage {

    private final DB db;

    private static final WriteOptions Sync = new WriteOptions().sync(true);

    private static final ReadOptions DontCache = new ReadOptions().fillCache(false);
    private static final ReadOptions Cache = new ReadOptions().fillCache(true);

    public KeyValueStorageLevelDB(String path) throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        options.compressionType(CompressionType.SNAPPY);
        options.writeBufferSize(128 * 1024 * 1024);
        options.cacheSize(512 * 1024 * 1024);

        db = JniDBFactory.factory.open(new File(path), options);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    @Override
    public void put(byte[] key, byte[] value) {
        // log.debug("put key={} value={}", Arrays.toString(key), Arrays.toString(value));
        db.put(key, value, Sync);
    }

    @Override
    public void put(Iterable<Entry<byte[], byte[]>> entries) throws IOException {
        WriteBatch writeBatch = db.createWriteBatch();
        for (Entry<byte[], byte[]> entry : entries) {
            // log.debug("batch put key={} value={}", LongPair.fromArray(entry.getKey()),
            // Arrays.toString(entry.getValue()));
            writeBatch.put(entry.getKey(), entry.getValue());
        }

        try {
            db.write(writeBatch, Sync);
        } finally {
            writeBatch.close();
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return db.get(key);
    }

    @Override
    public Entry<byte[], byte[]> getFloor(byte[] key) throws IOException {
        DBIterator iterator = db.iterator(Cache);

        try {
            // Position the iterator on the record whose key is >= to the supplied key
            iterator.seek(key);

            if (!iterator.hasNext()) {
                // There are no entries >= key
                iterator.seekToLast();
                if (iterator.hasNext()) {
                    return iterator.next();
                } else {
                    // Db is empty
                    return null;
                }
            }

            if (!iterator.hasPrev()) {
                // Iterator is on the 1st entry of the db and this entry key is >= to the target key
                return null;
            }

            return iterator.prev();
        } finally {
            iterator.close();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        db.delete(key, Sync);
    }

    @Override
    public void delete(Iterable<byte[]> keys) throws IOException {
        WriteBatch writeBatch = db.createWriteBatch();
        for (byte[] key : keys) {
            writeBatch.delete(key);
        }

        try {
            db.write(writeBatch, Sync);
        } finally {
            writeBatch.close();
        }
    }

    @Override
    public CloseableIterator<byte[]> keys() {
        final DBIterator iterator = db.iterator(DontCache);
        iterator.seekToFirst();

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public byte[] next() {
                Entry<byte[], byte[]> entry = iterator.next();
                if (entry != null) {
                    return entry.getKey();
                } else {
                    return null;
                }
            }

            @Override
            public void close() throws IOException {
                iterator.close();
            }
        };
    }

    @Override
    public long count() throws IOException {
        long count = 0;

        CloseableIterator<byte[]> iter = keys();
        try {
            while (iter.hasNext()) {
                ++count;
            }
        } finally {
            iter.close();
        }

        return count;
    }

    private final static Comparator<byte[]> ByteComparator = UnsignedBytes.lexicographicalComparator();

    @Override
    public CloseableIterator<byte[]> keys(byte[] firstKey, final byte[] lastKey) {
        final DBIterator iterator = db.iterator(DontCache);
        iterator.seek(firstKey);

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext() && ByteComparator.compare(iterator.peekNext().getKey(), lastKey) < 0;
            }

            @Override
            public byte[] next() {
                Entry<byte[], byte[]> entry = iterator.next();
                if (entry != null) {
                    return entry.getKey();
                } else {
                    return null;
                }
            }

            @Override
            public void close() throws IOException {
                iterator.close();
            }
        };
    }

    public CloseableIterator<Entry<byte[], byte[]>> iterator() {
        final DBIterator iterator = db.iterator(DontCache);
        iterator.seekToFirst();
        return new CloseableIterator<Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() throws IOException {
                return iterator.hasNext();
            }

            @Override
            public Entry<byte[], byte[]> next() throws IOException {
                return iterator.next();
            }

            @Override
            public void close() throws IOException {
                iterator.close();
            }
        };
    }
}
