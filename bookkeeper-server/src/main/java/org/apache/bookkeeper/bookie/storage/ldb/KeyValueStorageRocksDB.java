package org.apache.bookkeeper.bookie.storage.ldb;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorageFactory.DbConfigType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ChecksumType;
import org.rocksdb.CompressionType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.UnsignedBytes;

public class KeyValueStorageRocksDB implements KeyValueStorage {

    static KeyValueStorageFactory factory = new KeyValueStorageFactory() {
        @Override
        public KeyValueStorage newKeyValueStorage(String path, DbConfigType dbConfigType, ServerConfiguration conf)
                throws IOException {
            doUpgradeIfNeeded(path, dbConfigType, conf);
            KeyValueStorageRocksDB db = new KeyValueStorageRocksDB(path, dbConfigType, conf);
            doCreateRocksDbMarker(path);
            return db;
        }
    };

    private final RocksDB db;

    private final WriteOptions Sync;

    private final ReadOptions Cache;
    private final ReadOptions DontCache;
    private static final String ROCKSDB_MARKER = "rocksdb-enabled";

    private static final String ROCKSDB_LOG_LEVEL = "dbStorage_rocksDB_logLevel";
    private static final String ROCKSDB_WRITE_BUFFER_SIZE_MB = "dbStorage_rocksDB_writeBufferSizeMB";
    private static final String ROCKSDB_SST_SIZE_MB = "dbStorage_rocksDB_sstSizeInMB";
    private static final String ROCKSDB_BLOCK_SIZE = "dbStorage_rocksDB_blockSize";
    private static final String ROCKSDB_BLOOM_FILTERS_BITS_PER_KEY = "dbStorage_rocksDB_bloomFilterBitsPerKey";
    private static final String ROCKSDB_BLOCK_CACHE_SIZE = "dbStorage_rocksDB_blockCacheSize";
    private static final String ROCKSDB_NUM_LEVELS = "dbStorage_rocksDB_numLevels";
    private static final String ROCKSDB_NUM_FILES_IN_LEVEL0 = "dbStorage_rocksDB_numFilesInLevel0";
    private static final String ROCKSDB_MAX_SIZE_IN_LEVEL1_MB = "dbStorage_rocksDB_maxSizeInLevel1MB";

    public KeyValueStorageRocksDB(String path, DbConfigType dbConfigType, ServerConfiguration conf) throws IOException {
        try {
            RocksDB.loadLibrary();
        } catch (Throwable t) {
            throw new IOException("Failed to load RocksDB JNI library", t);
        }

        Options options = new Options().setCreateIfMissing(true);

        if (dbConfigType == DbConfigType.Huge) {
            long writeBufferSizeMB = conf.getInt(ROCKSDB_WRITE_BUFFER_SIZE_MB, 64);
            long sstSizeMB = conf.getInt(ROCKSDB_SST_SIZE_MB, 64);
            int numLevels = conf.getInt(ROCKSDB_NUM_LEVELS, -1);
            int numFilesInLevel0 = conf.getInt(ROCKSDB_NUM_FILES_IN_LEVEL0, 4);
            long maxSizeInLevel1MB = conf.getLong(ROCKSDB_MAX_SIZE_IN_LEVEL1_MB, 256);
            int blockSize = conf.getInt(ROCKSDB_BLOCK_SIZE, 64 * 1024);
            long blockCacheSize = conf.getInt(ROCKSDB_BLOCK_CACHE_SIZE, 256 * 1024 * 1024);
            int bloomFilterBitsPerKey = conf.getInt(ROCKSDB_BLOOM_FILTERS_BITS_PER_KEY, 10);

            options.setCompressionType(CompressionType.LZ4_COMPRESSION);
            options.setWriteBufferSize(writeBufferSizeMB * 1024 * 1024);
            options.setMaxWriteBufferNumber(4);
            if (numLevels > 0) {
                options.setNumLevels(numLevels);
            }
            options.setLevelZeroFileNumCompactionTrigger(numFilesInLevel0);
            options.setMaxBytesForLevelBase(maxSizeInLevel1MB * 1024 * 1024);
            options.setMaxBackgroundCompactions(16);
            options.setMaxBackgroundFlushes(16);
            options.setIncreaseParallelism(32);
            options.setMaxTotalWalSize(512 * 1024 * 1024);
            options.setMaxOpenFiles(-1);
            options.setTargetFileSizeBase(sstSizeMB * 1024 * 1024);
            options.setDeleteObsoleteFilesPeriodMicros(TimeUnit.HOURS.toMicros(1));

            BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
            tableOptions.setBlockSize(blockSize);
            tableOptions.setBlockCacheSize(blockCacheSize);
            tableOptions.setFormatVersion(2);
            tableOptions.setChecksumType(ChecksumType.kxxHash);
            if (bloomFilterBitsPerKey > 0) {
                tableOptions.setFilter(new BloomFilter(bloomFilterBitsPerKey, false));
            }

            // Options best suited for HDDs
            tableOptions.setCacheIndexAndFilterBlocks(true);
            options.setLevelCompactionDynamicLevelBytes(true);

            options.setTableFormatConfig(tableOptions);
        }

        // Configure log level
        String logLevel = conf.getString(ROCKSDB_LOG_LEVEL, "info");
        switch (logLevel) {
        case "debug":
            options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
            break;
        case "info":
            options.setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
            break;
        case "warn":
            options.setInfoLogLevel(InfoLogLevel.WARN_LEVEL);
            break;
        case "error":
            options.setInfoLogLevel(InfoLogLevel.ERROR_LEVEL);
            break;
        default:
            log.warn("Unrecognized RockDB log level: {}", logLevel);
        }

        // Keep log files for 1month
        options.setKeepLogFileNum(30);
        options.setLogFileTimeToRoll(TimeUnit.DAYS.toSeconds(1));

        try {
            db = RocksDB.open(options, path);
            options.dispose();
        } catch (RocksDBException e) {
            throw new IOException("Error open RocksDB database", e);
        }

        Sync = new WriteOptions().setSync(true);

        Cache = new ReadOptions().setFillCache(true);
        DontCache = new ReadOptions().setFillCache(false);
    }

    @Override
    public void close() throws IOException {
        db.close();
        Sync.dispose();
        Cache.dispose();
        DontCache.dispose();
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(Sync, key, value);
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
            db.remove(Sync, key);
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
                db.write(Sync, this);
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

    /**
     * Checks whether the DB was already created with RocksDB, otherwise copy all data from one database into a fresh
     * database.
     *
     * Useful when switching from LevelDB to RocksDB since it lets us to restart fresh and build a DB with the new
     * settings (SSTs sizes, levels, etc..)
     */
    private static void doUpgradeIfNeeded(String path, DbConfigType dbConfigType, ServerConfiguration conf)
            throws IOException {
        FileSystem fileSystem = FileSystems.getDefault();
        final Path rocksDbMarkerFile = fileSystem.getPath(path, ROCKSDB_MARKER);

        if (Files.exists(fileSystem.getPath(path))) {
            // Database already existing
            if (Files.exists(rocksDbMarkerFile)) {
                // Database was already created with RocksDB
                return;
            }
        } else {
            // Database not existing, no need to convert
            return;
        }

        log.info("Converting existing database to RocksDB: {}", path);
        long startTime = System.nanoTime();

        String rocksDbPath = path + ".rocksdb";
        KeyValueStorage source = new KeyValueStorageRocksDB(path, dbConfigType, conf);

        log.info("Opened existing db, starting copy");
        KeyValueStorage target = new KeyValueStorageRocksDB(rocksDbPath, dbConfigType, conf);

        // Copy into new database. Write in batches to speed up the insertion
        CloseableIterator<Entry<byte[], byte[]>> iterator = source.iterator();
        try {
            final int maxBatchSize = 10000;
            int currentBatchSize = 0;
            Batch batch = target.newBatch();

            while (iterator.hasNext()) {
                Entry<byte[], byte[]> entry = iterator.next();

                batch.put(entry.getKey(), entry.getValue());
                if (++currentBatchSize == maxBatchSize) {
                    batch.flush();
                    batch = target.newBatch();
                    currentBatchSize = 0;
                }
            }

            batch.flush();
        } finally {
            iterator.close();
            source.close();
            target.close();
        }

        FileUtils.deleteDirectory(new File(path));
        Files.move(fileSystem.getPath(rocksDbPath), fileSystem.getPath(path));

        // Create the marked to avoid conversion next time
        Files.createFile(rocksDbMarkerFile);

        log.info("Database conversion done. Total time: {}",
                DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)));
    }

    private static void doCreateRocksDbMarker(String path) throws IOException {
        try {
            Files.createFile(FileSystems.getDefault().getPath(path, ROCKSDB_MARKER));
        } catch (FileAlreadyExistsException e) {
            // Ignore
        }
    }

    private final static Comparator<byte[]> ByteComparator = UnsignedBytes.lexicographicalComparator();

    private static final Logger log = LoggerFactory.getLogger(KeyValueStorageRocksDB.class);
}
