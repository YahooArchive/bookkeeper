package org.apache.bookkeeper.bookie.storage.ldb;

import org.apache.bookkeeper.client.BKException.BKEntryTrimmedException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import static org.junit.Assert.*;

public class DbLedgerStorageBookieTest extends BookKeeperClusterTestCase {

    public DbLedgerStorageBookieTest() {
        super(1);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        baseConf.setProperty(DbLedgerStorage.TRIM_ENABLED, true);
    }

    @Test
    public void testTrimming() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);
        long entry0 = lh.addEntry("my-entry".getBytes());

        lh.asyncTrim(entry0);

        // Wait for async-trim to complete
        Thread.sleep(1000);

        try {
            lh.readEntries(entry0, entry0);
            fail("Entry should have been trimmed");
        } catch (BKEntryTrimmedException e) {
            // Ok
        }
    }

    @Test
    public void testRecoveryEmptyLedger() throws Exception {
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);

        // Force ledger close & recovery
        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.MAC, new byte[0]);

        assertEquals(0, lh2.getLength());
        assertEquals(-1, lh2.getLastAddConfirmed());
    }

    @Test
    public void testRecoveryTrimmedLedger() throws Exception {
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);
        for (int i = 0; i < 5; i++) {
            lh1.addEntry("entry".getBytes());
        }

        lh1.asyncTrim(4);
        Thread.sleep(100);

        // Force ledger close & recovery
        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.MAC, new byte[0]);

        assertEquals(0, lh2.getLength());
        assertEquals(-1, lh2.getLastAddConfirmed());
    }

    @Test
    public void testRecoveryPartiallyTrimmedLedger() throws Exception {
        LedgerHandle lh1 = bkc.createLedger(1, 1, DigestType.MAC, new byte[0]);
        for (int i = 0; i < 5; i++) {
            lh1.addEntry("entry".getBytes());
        }

        lh1.asyncTrim(2);
        Thread.sleep(100);

        // Force ledger close & recovery
        LedgerHandle lh2 = bkc.openLedger(lh1.getId(), DigestType.MAC, new byte[0]);

        assertEquals(4, lh2.getLastAddConfirmed());
    }
}
