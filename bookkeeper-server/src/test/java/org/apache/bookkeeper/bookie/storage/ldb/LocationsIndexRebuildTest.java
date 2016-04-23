package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.CheckpointSource;
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

@RunWith(Parameterized.class)
public class LocationsIndexRebuildTest {

    private final boolean rocksDBEnabled;

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    public LocationsIndexRebuildTest(boolean rocksDBEnabled) {
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
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setProperty(DbLedgerStorage.ROCKSDB_ENABLED, rocksDBEnabled);
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());

        DbLedgerStorage ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        // Insert some ledger & entries in the storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            ledgerStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            ledgerStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(128);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ledgerStorage.addEntry(entry);
            }
        }

        ledgerStorage.flush();
        ledgerStorage.shutdown();

        // Rebuild index through the tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "rebuild-db-ledger-locations-index" });

        Assert.assertEquals(0, res);

        // Verify that db index has the same entries
        ledgerStorage = new DbLedgerStorage();
        ledgerStorage.initialize(conf, null, ledgerDirsManager, ledgerDirsManager, checkpointSource,
                NullStatsLogger.INSTANCE);

        Set<Long> ledgers = Sets.newTreeSet(ledgerStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Sets.newTreeSet(Lists.newArrayList(0l, 1l, 2l, 3l, 4l)), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, ledgerStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(ledgerStorage.readMasterKey(ledgerId)));

            ByteBuf lastEntry = ledgerStorage.getLastEntry(ledgerId);
            assertEquals(ledgerId, lastEntry.readLong());
            long lastEntryId = lastEntry.readLong();
            assertEquals(99, lastEntryId);

            for (long entryId = 0; entryId < 100; entryId++) {
                ByteBuf entry = Unpooled.buffer(1024);
                entry.writeLong(ledgerId);
                entry.writeLong(entryId);
                entry.writeBytes(("entry-" + entryId).getBytes());

                ByteBuf result = ledgerStorage.getEntry(ledgerId, entryId);
                Assert.assertEquals(entry, result);
            }
        }

        ledgerStorage.shutdown();
        FileUtils.forceDelete(tmpDir);
    }
}
