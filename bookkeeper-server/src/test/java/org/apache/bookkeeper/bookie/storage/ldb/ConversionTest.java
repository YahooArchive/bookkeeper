package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoLedgerException;
import org.apache.bookkeeper.bookie.BookieShell;
import org.apache.bookkeeper.bookie.InterleavedLedgerStorage;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ConversionTest {

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
        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf);

        InterleavedLedgerStorage interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, NullStatsLogger.INSTANCE);

        // Insert some ledger & entries in the interleaved storage
        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            interleavedStorage.setMasterKey(ledgerId, ("ledger-" + ledgerId).getBytes());
            interleavedStorage.setFenced(ledgerId);

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuffer entry = ByteBuffer.allocate(128);
                entry.putLong(ledgerId);
                entry.putLong(entryId);
                entry.put(("entry-" + entryId).getBytes());
                entry.flip();

                interleavedStorage.addEntry(entry);
            }
        }

        interleavedStorage.flush();
        interleavedStorage.shutdown();

        // Run conversion tool
        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(new String[] { "upgrade-db-storage" });

        Assert.assertEquals(0, res);

        // Verify that db index has the same entries
        DbLedgerStorage dbStorage = new DbLedgerStorage();
        dbStorage.initialize(conf, null, ledgerDirsManager, NullStatsLogger.INSTANCE);

        interleavedStorage = new InterleavedLedgerStorage();
        interleavedStorage.initialize(conf, null, ledgerDirsManager, NullStatsLogger.INSTANCE);

        List<Long> ledgers = Lists.newArrayList(dbStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Lists.newArrayList(0l, 1l, 2l, 3l, 4l), ledgers);

        ledgers = Lists.newArrayList(interleavedStorage.getActiveLedgersInRange(0, Long.MAX_VALUE));
        Assert.assertEquals(Lists.newArrayList(), ledgers);

        for (long ledgerId = 0; ledgerId < 5; ledgerId++) {
            Assert.assertEquals(true, dbStorage.isFenced(ledgerId));
            Assert.assertEquals("ledger-" + ledgerId, new String(dbStorage.readMasterKey(ledgerId)));

            for (long entryId = 0; entryId < 10000; entryId++) {
                ByteBuffer entry = ByteBuffer.allocate(128);
                entry.putLong(ledgerId);
                entry.putLong(entryId);
                entry.put(("entry-" + entryId).getBytes());
                entry.flip();

                Assert.assertEquals(entry, dbStorage.getEntry(ledgerId, entryId));

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
    }
}
