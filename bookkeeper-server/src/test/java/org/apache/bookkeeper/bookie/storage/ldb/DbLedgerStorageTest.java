package org.apache.bookkeeper.bookie.storage.ldb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.EntryTrimmedException;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage.EntryLocation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class DbLedgerStorageTest {

    private DbLedgerStorage storage;
    private File tmpDir;

    @Before
    public void setup() throws Exception {
        tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setAllowLoopback(true);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new Bookie(conf);

        storage = (DbLedgerStorage) bookie.getLedgerStorage();
    }

    @After
    public void teardown() throws Exception {
        tmpDir.delete();
    }

    @Test
    public void simple() throws Exception {
        assertEquals(false, storage.ledgerExists(3));
        try {
            storage.isFenced(3);
            fail("should have failed");
        } catch (Bookie.NoLedgerException nle) {
            // OK
        }
        assertEquals(false, storage.ledgerExists(3));
        try {
            storage.setFenced(3);
            fail("should have failed");
        } catch (Bookie.NoLedgerException nle) {
            // OK
        }
        storage.setMasterKey(3, "key".getBytes());
        try {
            storage.setMasterKey(3, "other-key".getBytes());
            fail("should have failed");
        } catch (IOException ioe) {
            assertTrue(ioe.getCause() instanceof BookieException.BookieIllegalOpException);
        }
        // setting the same key is NOOP
        storage.setMasterKey(3, "key".getBytes());
        assertEquals(true, storage.ledgerExists(3));
        assertEquals(true, storage.setFenced(3));
        assertEquals(true, storage.isFenced(3));
        assertEquals(false, storage.setFenced(3));

        storage.setMasterKey(4, "key".getBytes());
        assertEquals(false, storage.isFenced(4));
        assertEquals(true, storage.ledgerExists(4));

        assertEquals("key", new String(storage.readMasterKey(4)));

        assertEquals(Lists.newArrayList(3l, 4l), Lists.newArrayList(storage.getActiveLedgersInRange(0, 100)));
        assertEquals(Lists.newArrayList(3l, 4l), Lists.newArrayList(storage.getActiveLedgersInRange(3, 100)));
        assertEquals(Lists.newArrayList(3l), Lists.newArrayList(storage.getActiveLedgersInRange(0, 4)));

        // Add / read entries
        ByteBuffer entry = ByteBuffer.allocate(1024);
        entry.putLong(4); // ledger id
        entry.putLong(1); // entry id
        entry.put("entry-1".getBytes());
        entry.flip();

        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());

        assertEquals(1, storage.addEntry(entry.duplicate()));

        assertEquals(true, ((DbLedgerStorage) storage).isFlushRequired());

        // Read from write cache
        ByteBuffer res = storage.getEntry(4, 1);
        assertEquals(entry, res);

        storage.flush();

        assertEquals(false, ((DbLedgerStorage) storage).isFlushRequired());

        // Read from db
        res = storage.getEntry(4, 1);
        assertEquals(entry, res);

        try {
            storage.getEntry(4, 2);
            fail("Should have thrown exception");
        } catch (NoEntryException e) {
            // ok
        }

        ByteBuffer entry2 = ByteBuffer.allocate(1024);
        entry2.putLong(4); // ledger id
        entry2.putLong(2); // entry id
        entry2.put("entry-2".getBytes());
        entry2.flip();

        storage.addEntry(entry2);

        // Read last entry in ledger
        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry2, res);

        ByteBuffer entry3 = ByteBuffer.allocate(1024);
        entry3.putLong(4); // ledger id
        entry3.putLong(3); // entry id
        entry3.put("entry-3".getBytes());
        entry3.flip();
        storage.addEntry(entry3);

        ByteBuffer entry4 = ByteBuffer.allocate(1024);
        entry4.putLong(4); // ledger id
        entry4.putLong(4); // entry id
        entry4.put("entry-4".getBytes());
        entry4.flip();
        storage.addEntry(entry4);

        // Trimming should be disabled by default
        storage.trimEntries(4, 3);
        res = storage.getEntry(4, 3);
        assertEquals(entry3, res);

        res = storage.getEntry(4, 4);
        assertEquals(entry4, res);

        // Delete
        assertEquals(true, storage.ledgerExists(4));
        storage.deleteLedger(4);
        assertEquals(false, storage.ledgerExists(4));

        try {
            storage.getEntry(4, 4);
            fail("Should have thrown exception");
        } catch (NoEntryException e) {
            // ok
        }

        storage.addEntry(entry2);
        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry2, res);

        // Get last entry from storage
        storage.flush();

        try {
            storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
            fail("should have failed");
        } catch (NoEntryException e) {
            // Ok, ledger 4 was already deleted
        }

        storage.setMasterKey(4, "key".getBytes());

        storage.deleteLedger(4);
        try {
            storage.getEntry(4, 2);
            fail("Should have thrown exception");
        } catch (NoEntryException e) {
            // ok
        }

        storage.shutdown();
    }

    @Test
    public void testBookieCompaction() throws Exception {
        storage.setMasterKey(4, "key".getBytes());

        ByteBuffer entry3 = ByteBuffer.allocate(1024);
        entry3.putLong(4); // ledger id
        entry3.putLong(3); // entry id
        entry3.put("entry-3".getBytes());
        entry3.flip();
        storage.addEntry(entry3);

        // Simulate bookie compaction
        EntryLogger entryLogger = ((DbLedgerStorage) storage).getEntryLogger();
        // Rewrite entry-3
        ByteBuffer newEntry3 = ByteBuffer.allocate(1024);
        newEntry3.putLong(4); // ledger id
        newEntry3.putLong(3); // entry id
        newEntry3.put("new-entry-3".getBytes());
        newEntry3.flip();
        long location = entryLogger.addEntry(4, newEntry3.duplicate(), false);

        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(4, 3, location));
        storage.updateEntriesLocations(locations);

        ByteBuffer res = storage.getEntry(4, 3);
        assertEquals(newEntry3, res);

        storage.shutdown();
    }

    @Test
    public void doubleDirectoryError() throws Exception {
        int gcWaitTime = 1000;
        ServerConfiguration conf = new ServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setAllowLoopback(true);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setLedgerDirNames(new String[] { "dir1", "dir2" });

        try {
            new Bookie(conf);
            fail("Should have failed because of the 2 directories");
        } catch (IllegalArgumentException e) {
            // ok
        }

    }

    @Test
    public void testRewritingEntries() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        try {
            storage.getEntry(1, -1);
            fail("Should throw exception");
        } catch (Bookie.NoEntryException e) {
            // ok
        }

        ByteBuffer entry1 = ByteBuffer.allocate(1024);
        entry1.putLong(1); // ledger id
        entry1.putLong(1); // entry id
        entry1.put("entry-1".getBytes());
        entry1.flip();

        storage.addEntry(entry1);
        storage.flush();

        ByteBuffer newEntry1 = ByteBuffer.allocate(1024);
        newEntry1.putLong(1); // ledger id
        newEntry1.putLong(1); // entry id
        newEntry1.put("new-entry-1".getBytes());
        newEntry1.flip();

        storage.addEntry(newEntry1);
        storage.flush();

        ByteBuffer response = storage.getEntry(1, 1);
        assertEquals(newEntry1, response);
    }

    @Test
    public void testEntriesOutOfOrder() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        ByteBuffer entry2 = ByteBuffer.allocate(1024);
        entry2.putLong(1); // ledger id
        entry2.putLong(2); // entry id
        entry2.put("entry-2".getBytes());
        entry2.flip();

        storage.addEntry(entry2);

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (EntryTrimmedException e) {
            // Ok, entry doesn't exist
        }

        assertEquals(entry2, storage.getEntry(1, 2));

        ByteBuffer entry1 = ByteBuffer.allocate(1024);
        entry1.putLong(1); // ledger id
        entry1.putLong(1); // entry id
        entry1.put("entry-1".getBytes());
        entry1.flip();

        storage.addEntry(entry1);

        assertEquals(entry1, storage.getEntry(1, 1));
        assertEquals(entry2, storage.getEntry(1, 2));

        storage.flush();

        assertEquals(entry1, storage.getEntry(1, 1));
        assertEquals(entry2, storage.getEntry(1, 2));
    }

    @Test
    public void testEntriesOutOfOrderWithFlush() throws Exception {
        storage.setMasterKey(1, "key".getBytes());

        ByteBuffer entry2 = ByteBuffer.allocate(1024);
        entry2.putLong(1); // ledger id
        entry2.putLong(2); // entry id
        entry2.put("entry-2".getBytes());
        entry2.flip();

        storage.addEntry(entry2);

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (EntryTrimmedException e) {
            // Ok, entry doesn't exist
        }

        assertEquals(entry2, storage.getEntry(1, 2));

        storage.flush();

        try {
            storage.getEntry(1, 1);
            fail("Entry doesn't exist");
        } catch (EntryTrimmedException e) {
            // Ok, entry doesn't exist
        }

        assertEquals(entry2, storage.getEntry(1, 2));

        ByteBuffer entry1 = ByteBuffer.allocate(1024);
        entry1.putLong(1); // ledger id
        entry1.putLong(1); // entry id
        entry1.put("entry-1".getBytes());
        entry1.flip();

        storage.addEntry(entry1);

        assertEquals(entry1, storage.getEntry(1, 1));
        assertEquals(entry2, storage.getEntry(1, 2));

        storage.flush();

        assertEquals(entry1, storage.getEntry(1, 1));
        assertEquals(entry2, storage.getEntry(1, 2));
    }

}
