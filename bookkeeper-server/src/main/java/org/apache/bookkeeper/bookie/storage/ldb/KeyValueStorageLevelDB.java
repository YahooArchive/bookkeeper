package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

import org.apache.bookkeeper.conf.ServerConfiguration;
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

    static KeyValueStorageFactory factory = new KeyValueStorageFactory() {
        @Override
        public KeyValueStorage newKeyValueStorage(String path, DbConfigType dbConfigType, ServerConfiguration conf) throws IOException {
            return new KeyValueStorageLevelDB(path);
        }
    };

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
                iter.next();
                ++count;
            }
        } finally {
            iter.close();
        }

        return count;
    }

    @Override
    public Batch newBatch() {
        return new LevelDBBatch();
    }

    private class LevelDBBatch implements Batch {

        WriteBatch ldbBatch = db.createWriteBatch();

        @Override
        public void put(byte[] key, byte[] value) {
            ldbBatch.put(key, value);
        }

        @Override
        public void remove(byte[] key) {
            ldbBatch.delete(key);
        }

        @Override
        public void flush() throws IOException {
            try {
                db.write(ldbBatch, Sync);
            } finally {
                ldbBatch.close();
            }
        }
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
