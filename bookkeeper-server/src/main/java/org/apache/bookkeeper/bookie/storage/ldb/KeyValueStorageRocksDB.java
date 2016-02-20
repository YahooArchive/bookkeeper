package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map.Entry;

import org.rocksdb.CompressionType;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.google.common.primitives.UnsignedBytes;

public class KeyValueStorageRocksDB implements KeyValueStorage {

    static KeyValueStorageFactory factory = new KeyValueStorageFactory() {
        @Override
        public KeyValueStorage newKeyValueStorage(String path) throws IOException {
            return new KeyValueStorageRocksDB(path);
        }
    };

    private final RocksDB db;

    private final WriteOptions DontSync;

    private final ReadOptions Cache;
    private final ReadOptions DontCache;

    private final FlushOptions FlushAndWait;

    public KeyValueStorageRocksDB(String path) throws IOException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            throw new IOException("Failed to load RocksDB JNI librayry", t);
        }

        Options options = new Options().setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setWriteBufferSize(32 * 1024 * 1024);
        options.setMaxWriteBufferNumber(4);

        try {
            db = RocksDB.open(options, path);
            options.dispose();
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
        }

        DontSync = new WriteOptions().setSync(false).setDisableWAL(true);

        Cache = new ReadOptions().setFillCache(true);
        DontCache = new ReadOptions().setFillCache(false);

        FlushAndWait = new FlushOptions().setWaitForFlush(true);
    }

    @Override
    public void close() throws IOException {
        db.close();
        DontSync.dispose();
        Cache.dispose();
        DontCache.dispose();
        FlushAndWait.dispose();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(DontSync, key, value);

            db.flush(FlushAndWait);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB put", e);
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB get", e);
        }
    }

    @Override
    public Entry<byte[], byte[]> getFloor(byte[] key) throws IOException {
        RocksIterator iterator = db.newIterator(Cache);

        try {
            // Position the iterator on the record whose key is >= to the supplied key
            iterator.seek(key);

            if (!iterator.isValid()) {
                // There are no entries >= key
                iterator.seekToLast();
                if (iterator.isValid()) {
                    return new EntryWrapper(iterator.key(), iterator.value());
                } else {
                    // Db is empty
                    return null;
                }
            }

            iterator.prev();

            if (!iterator.isValid()) {
                // Iterator is on the 1st entry of the db and this entry key is >= to the target key
                return null;
            } else {
                return new EntryWrapper(iterator.key(), iterator.value());
            }
        } finally {
            iterator.dispose();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        try {
            db.remove(DontSync, key);

            db.flush(FlushAndWait);
        } catch (RocksDBException e) {
            throw new IOException("Error in RocksDB delete", e);
        }
    }

    @Override
    public CloseableIterator<byte[]> keys() {
        final RocksIterator iterator = db.newIterator(Cache);
        iterator.seekToFirst();

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid();
            }

            @Override
            public byte[] next() {
                checkArgument(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() throws IOException {
                iterator.dispose();
            }
        };
    }

    @Override
    public CloseableIterator<byte[]> keys(byte[] firstKey, byte[] lastKey) {
        final RocksIterator iterator = db.newIterator(Cache);
        iterator.seek(firstKey);

        return new CloseableIterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.isValid() && ByteComparator.compare(iterator.key(), lastKey) < 0;
            }

            @Override
            public byte[] next() {
                checkArgument(iterator.isValid());
                byte[] key = iterator.key();
                iterator.next();
                return key;
            }

            @Override
            public void close() throws IOException {
                iterator.dispose();
            }
        };
    }

    @Override
    public CloseableIterator<Entry<byte[], byte[]>> iterator() {
        final RocksIterator iterator = db.newIterator(DontCache);
        iterator.seekToFirst();
        final EntryWrapper entryWrapper = new EntryWrapper();

        return new CloseableIterator<Entry<byte[], byte[]>>() {
            @Override
            public boolean hasNext() throws IOException {
                return iterator.isValid();
            }

            @Override
            public Entry<byte[], byte[]> next() throws IOException {
                checkArgument(iterator.isValid());
                entryWrapper.key = iterator.key();
                entryWrapper.value = iterator.value();
                iterator.next();
                return entryWrapper;
            }

            @Override
            public void close() throws IOException {
                iterator.dispose();
            }
        };
    }

    @Override
    public long count() throws IOException {
        try {
            return db.getLongProperty("rocksdb.estimate-num-keys");
        } catch (RocksDBException e) {
            throw new IOException("Error in getting records count", e);
        }
    }

    @Override
    public Batch newBatch() {
        return new RocksDBBatch();
    }

    private class RocksDBBatch extends WriteBatch implements Batch {
        @Override
        public void flush() throws IOException {
            try {
                db.write(DontSync, this);
                db.flush(FlushAndWait);
            } catch (RocksDBException e) {
                throw new IOException("Failed to flush RocksDB batch", e);
            } finally {
                dispose();
            }
        }
    }

    private static class EntryWrapper implements Entry<byte[], byte[]> {
        private byte[] key;
        private byte[] value;

        public EntryWrapper() {
        }

        public EntryWrapper(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public byte[] setValue(byte[] value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] getValue() {
            return value;
        }

        @Override
        public byte[] getKey() {
            return key;
        }
    }

    private final static Comparator<byte[]> ByteComparator = UnsignedBytes.lexicographicalComparator();
}
