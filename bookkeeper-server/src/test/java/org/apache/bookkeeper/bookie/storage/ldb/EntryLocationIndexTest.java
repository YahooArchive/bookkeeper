package org.apache.bookkeeper.bookie.storage.ldb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.bookkeeper.bookie.Bookie.NoEntryException;
import org.apache.bookkeeper.bookie.storage.ldb.EntryLocationIndex.EntryRange;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

@RunWith(Parameterized.class)
public class EntryLocationIndexTest {

    private final KeyValueStorageFactory storageFactory;
    
    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { {KeyValueStorageLevelDB.factory }, {KeyValueStorageRocksDB.factory}});
    }
    
    public EntryLocationIndexTest(KeyValueStorageFactory storageFactory) {
        this.storageFactory = storageFactory;
    }
    
    @Test
    public void entryRangeTest() {
        EntryRange er = new EntryRange(1, 0, 10);

        assertEquals("(1,0,10)", er.toString());

        assertEquals(0, er.compareTo(new EntryRange(1, 0, 10)));
        assertEquals(-1, er.compareTo(new EntryRange(1, 11, 20)));
        assertEquals(-1, er.compareTo(new EntryRange(2, 0, 10)));

        assertEquals(+1, new EntryRange(2, 0, 10).compareTo(er));

        assertEquals(0, new LongPair(1, 0).compareTo(er));
        assertEquals(0, new LongPair(1, 5).compareTo(er));
        assertEquals(0, new LongPair(1, 1).compareTo(er));

        assertEquals(-1, new LongPair(0, 10).compareTo(er));
        assertEquals(+1, new LongPair(2, 10).compareTo(er));

        assertEquals(-1, new LongPair(0, 10).compareTo(new EntryRange(0, 11, 20)));
        assertEquals(+1, new LongPair(0, 10).compareTo(new EntryRange(0, 1, 9)));
    }

    @Test
    public void ledgerIndexPageTest() {
        List<LongPair> entries1 = Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1));
        List<LongPair> entries2 = Lists.newArrayList(new LongPair(0, 0), new LongPair(1, 1), new LongPair(2, 2));
        List<LongPair> entries3 = Lists.newArrayList(new LongPair(1, 1), new LongPair(2, 2));
        LedgerIndexPage p1 = new LedgerIndexPage(1, entries1);

        assertEquals(p1, new LedgerIndexPage(1, entries1));
        assertEquals(p1.hashCode(), new LedgerIndexPage(1, entries1).hashCode());

        assertFalse(p1.equals(new LedgerIndexPage(2, entries1)));
        assertFalse(p1.equals(new LedgerIndexPage(1, entries2)));
        assertFalse(p1.equals(new LedgerIndexPage(1, entries3)));
        assertFalse(p1.equals(new LedgerIndexPage(1, entries3)));
        assertFalse(p1.equals(new Integer(1)));

        try {
            p1.setValue(new byte[] {});
            fail("Should have thrown exception");
        } catch (UnsupportedOperationException e) {
            // ok
        }
    }

    @Test
    public void deleteLedgerTest() throws Exception {
        File tmpDir = File.createTempFile("bkTest", ".dir");
        tmpDir.delete();
        tmpDir.mkdir();
        tmpDir.deleteOnExit();

        EntryLocationIndex idx = new EntryLocationIndex(storageFactory, tmpDir.getAbsolutePath(),
                NullStatsLogger.INSTANCE, 1 * 1024);

        Multimap<Long, LongPair> locations = ArrayListMultimap.create();

        // Add some dummy indexes
        locations.put(40312l, new LongPair(0, 1));
        locations.put(40313l, new LongPair(10, 2));
        locations.put(40320l, new LongPair(0, 3));
        idx.addLocations(locations);

        locations.clear();

        // Add more indexes in a different batch
        locations.put(40313l, new LongPair(11, 5));
        locations.put(40313l, new LongPair(12, 6));
        locations.put(40320l, new LongPair(1, 7));
        locations.put(40312l, new LongPair(3, 4));
        idx.addLocations(locations);

        idx.delete(40313);

        assertEquals(1, idx.getLocation(40312, 0));
        assertEquals(4, idx.getLocation(40312, 3));
        assertEquals(3, idx.getLocation(40320, 0));
        assertEquals(7, idx.getLocation(40320, 1));

        try {
            idx.getLocation(40313, 10);
            fail("should have failed");
        } catch (NoEntryException e) {
            // Ok
        }

        try {
            idx.getLocation(40313, 11);
            fail("should have failed");
        } catch (NoEntryException e) {
            // Ok
        }

        try {
            idx.getLocation(40313, 12);
            fail("should have failed");
        } catch (NoEntryException e) {
            // Ok
        }

        idx.close();
    }
}
