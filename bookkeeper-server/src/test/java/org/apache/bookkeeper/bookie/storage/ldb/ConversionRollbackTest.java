package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ConversionRollbackTest {

    CheckpointSource checkpointSource = new CheckpointSource() {
        @Override
        public Checkpoint newCheckpoint() {
            return Checkpoint.MAX;
        }

        @Override
        public void checkpointComplete(Checkpoint checkpoint, boolean compact) throws IOException {
        }
    };

    @Test
    public void convertFromDbStorageToInterleaved() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        System.out.println(tmpDir);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());

        DbLedgerStorage dbStorage = new DbLedgerStorage();
        dbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        // Insert some ledger & entries in the dbStorage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            dbStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            dbStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                dbStorage.addEntry(entry);
            }
        }

        dbStorage.flush();
        dbStorage.shutdown();

        // Run conversion tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "convert-to-interleaved-storage" });

        Assert.assertEquals(0, res);

        // Verify that interleaved storage index has the same entries
        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        Set<Long> ledgers = Sets.newTreeSet(interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0l, 1l, 2l, 3l, 4l)), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, interleavedStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(interleavedStorage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = interleavedStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
            }
        }

        interleavedStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }

    @Test
    public void convertFromRocksDbToLevelDb() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        System.out.println(tmpDir);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());

        DbLedgerStorage rocksDbStorage = new DbLedgerStorage();
        conf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, true);
        rocksDbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        // Insert some ledger & entries in the rocksDbStorage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            rocksDbStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            rocksDbStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                rocksDbStorage.addEntry(entry);
            }
        }

        rocksDbStorage.flush();
        rocksDbStorage.shutdown();

        // Run conversion tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "convert-rocksdb-to-leveldb-storage" });

        Assert.assertEquals(0, res);

        // Verify that levelDB storage index has the same entries
        DbLedgerStorage levelDbStorage = new DbLedgerStorage();
        conf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, false);
        levelDbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        Set<Long> ledgers = Sets.newTreeSet(levelDbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0l, 1l, 2l, 3l, 4l)), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, levelDbStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(levelDbStorage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = levelDbStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
            }
        }

        levelDbStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }
}
