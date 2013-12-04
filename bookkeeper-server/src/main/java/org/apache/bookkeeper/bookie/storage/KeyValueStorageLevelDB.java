package org.apache.bookkeeper.bookie.storage;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

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
    public Iterable<byte[]> keys() {
        final DBIterator iterator = db.iterator(DontCache);
        iterator.seekToFirst();

        return new Iterable<byte[]>() {
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {

                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    public byte[] next() {
                        Entry<byte[], byte[]> entry = iterator.next();
                        if (entry != null) {
                            return entry.getKey();
                        } else {
                            return null;
                        }
                    }

                    public void remove() {
                        throw new UnsupportedOperationException("Cannot remove from iterator");
                    }
                };
            }
        };
    }

    @Override
    public Iterable<byte[]> keys(byte[] firstKey, final byte[] lastKey) {
        final DBIterator iterator = db.iterator(DontCache);
        iterator.seek(firstKey);

        return new Iterable<byte[]>() {
            public Iterator<byte[]> iterator() {
                return new Iterator<byte[]>() {

                    public boolean hasNext() {
                        return iterator.hasNext() && compare(iterator.peekNext().getKey(), lastKey) < 0;
                    }

                    public byte[] next() {
                        Entry<byte[], byte[]> entry = iterator.next();
                        if (entry != null) {
                            return entry.getKey();
                        } else {
                            return null;
                        }
                    }

                    public void remove() {
                        throw new UnsupportedOperationException("Cannot remove from iterator");
                    }
                };
            }
        };
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
        DBIterator iterator = db.iterator(DontCache);
        iterator.seekToFirst();
        return iterator;
    }

    private static int compare(byte[] key1, byte[] key2) {
        int minLen = Math.min(key1.length, key2.length);

        for (int i = 0; i < minLen; i++) {
            if (key1[i] != key2[i]) {
                return Byte.compare(key1[i], key2[i]);
            }
        }

        return Integer.compare(key1.length, key2.length);
    }
}
