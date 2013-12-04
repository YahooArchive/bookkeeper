package org.apache.bookkeeper.bookie.storage.ldb;

import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.client.BKException.BKEntryTrimmedException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

public class DbLedgerStorageBookieTest extends BookKeeperClusterTestCase {

    public DbLedgerStorageBookieTest() {
        super(3);
        baseConf.setLedgerStorageClass(DbLedgerStorage.class.getName());
    }

    @Test
    public void testTrimming() throws Exception {
        LedgerHandle lh = bkc.createLedger(DigestType.MAC, new byte[0]);
        long entry0 = lh.addEntry("my-entry".getBytes());

        lh.asyncTrim(entry0);

        // Wait for async-trim to complete
        Thread.sleep(100);

        try {
            lh.readEntries(entry0, entry0);
            fail("Entry should have been trimmed");
        } catch (BKEntryTrimmedException e) {
            // Ok
        }
    }

}
