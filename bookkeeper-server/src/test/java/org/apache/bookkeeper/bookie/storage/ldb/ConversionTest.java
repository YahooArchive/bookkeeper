package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@RunWith(Parameterized.class)
public class ConversionTest {

    private final boolean rocksDBEnabled;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    public ConversionTest(boolean rocksDBEnabled) {
        this.rocksDBEnabled = rocksDBEnabled;
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

    @Test
    public void test() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        System.out.println(tmpDir);

        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        conf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, rocksDBEnabled);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        // Insert some ledger & entries in the interleaved storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            interleavedStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                interleavedStorage.addEntry(entry);
            }
        }

        interleavedStorage.flush();
        interleavedStorage.shutdown();

        // Run conversion tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "convert-to-db-storage" });

        Assert.assertEquals(0, res);

        // Verify that db index has the same entries
        DbLedgerStorage dbStorage = new DbLedgerStorage();
        dbStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        Set<Long> ledgers = Sets.newTreeSet(dbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0l, 1l, 2l, 3l, 4l)), ledgers);

        ledgers = Sets.newTreeSet(interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, dbStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(dbStorage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = dbStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);

                try {
                    interleavedStorage.getEntry(ledgerId, entryId);
                    Assert.fail("entry should not exist");
                } catch (NoLedgerException e) {
                    // Ok
                }
            }
        }

        interleavedStorage.shutdown();
        dbStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }
}
