package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class DbLedgerStorageRocksDBUpgrade {

    private File tmpDir;

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        System.err.println("Writing to " + tmpDir);
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
    }

    DbLedgerStorage createStorage(boolean rocksDBEnabled) throws Exception {
        int gcWaitTime = 1000;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setAllowLoopback(true);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, rocksDBEnabled);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());

        DbLedgerStorage storage = new DbLedgerStorage();
        storage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        return storage;
    }

    @After
    public void teardown() throws Exception {
        FileUtils.deleteDirectory(tmpDir);
    }

    @Test
    public void simple() throws Exception {
        DbLedgerStorage levelDbStorage = createStorage(false);

        insertEntries(levelDbStorage, 0, 5);

        levelDbStorage.flush();
        levelDbStorage.shutdown();

        // Re-open with LevelDB to force sst creation
        levelDbStorage = createStorage(false);
        insertEntries(levelDbStorage, 5, 10);

        levelDbStorage.flush();
        levelDbStorage.shutdown();

        // Re-open with RocksDB storage
        DbLedgerStorage rocksDBStorage = createStorage(true);
        verifyEntries(rocksDBStorage, 0, 10);

        // Insert more entries
        insertEntries(rocksDBStorage, 10, 15);
        verifyEntries(rocksDBStorage, 0, 15);
        rocksDBStorage.flush();
        verifyEntries(rocksDBStorage, 0, 15);
        rocksDBStorage.shutdown();
    }

    private void insertEntries(DbLedgerStorage storage, int firstLedger, int lastLedger) throws Exception {
        // Insert some ledger & entries in the storage
        for (long ledgerId = firstLedger; ledgerId < lastLedger; ledgerId++) {
            storage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            storage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                storage.addEntry(entry);
            }
        }
    }

    private void verifyEntries(DbLedgerStorage storage, int firstLedger, int lastLedger) throws Exception {
        // Verify that db index has the same entries
        Set<Long> ledgers = Sets.newTreeSet(storage.getActiveLedgersInRange(firstLedger, lastLedger));

        Set<Long> expectedSet = Sets.newTreeSet();
        for (long i = firstLedger; i < lastLedger; i++) {
            expectedSet.add(i);
        }
        Assert.assertEquals(expectedSet, ledgers);

        for (long ledgerId = firstLedger; ledgerId < lastLedger; ledgerId++) {
            Assert.assertEquals(true, storage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(storage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = storage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
                result.release();
            }
        }
    }

    CheckpointSource checkpointSource = new CheckpointSource() {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        }
    };
}
