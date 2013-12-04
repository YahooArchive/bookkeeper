package org.apache.bookkeeper.bookie.storage;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LedgerStorage.EntryLocation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.junit.Test;

import com.google.common.collect.Lists;

public class DbLedgerStorageTest {

    @Test
    public void simple() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
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

        LedgerStorage storage = bookie.getLedgerStorage();
        assertTrue(storage instanceof DbLedgerStorage);

        assertEquals(false, storage.ledgerExists(3));
        assertEquals(false, storage.isFenced(3));
        assertEquals(false, storage.ledgerExists(3));
        assertEquals(true, storage.setFenced(3));
        assertEquals(true, storage.ledgerExists(3));
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

        assertEquals(false, storage.isFlushRequired());

        assertEquals(1, storage.addEntry(entry.duplicate()));

        assertEquals(true, storage.isFlushRequired());

        // Read from write cache
        ByteBuffer res = storage.getEntry(4, 1);
        assertEquals(entry, res);

        storage.flush();

        assertEquals(false, storage.isFlushRequired());

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

        // Trimming
        storage.trimEntries(4, 3);

        try {
            storage.getEntry(4, 3);
            fail("Should have thrown exception");
        } catch (NoEntryException e) {
            // ok
        }

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

        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry2, res);

        // Get last entry from cache
        res = storage.getEntry(4, BookieProtocol.LAST_ADD_CONFIRMED);
        assertEquals(entry2, res);

        storage.setMasterKey(4, "key".getBytes());

        storage.deleteAllLedgers();
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
        File tmpDir = File.createTempFile("bkTest", ".dir");
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

        LedgerStorage storage = bookie.getLedgerStorage();
        assertTrue(storage instanceof DbLedgerStorage);

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
        long location = entryLogger.addEntry(4, newEntry3.duplicate());

        List<EntryLocation> locations = Lists.newArrayList(new EntryLocation(4, 3, location));
        storage.updateEntriesLocations(locations);

        ByteBuffer res = storage.getEntry(4, 3);
        assertEquals(newEntry3, res);

        storage.shutdown();
    }

    @Test
    public void doubleDirectoryError() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

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

}
