package org.apache.bookkeeper.bookie.storage.ldb;

import static junit.framework.Assert.*;

import java.util.List;

import org.apache.bookkeeper.bookie.storage.ldb.LedgerIndexPage;
import org.apache.bookkeeper.bookie.storage.ldb.LongPair;
import org.apache.bookkeeper.bookie.storage.ldb.EntryLocationIndex.EntryRange;
import org.junit.Test;

import com.google.common.collect.Lists;

public class EntryLocationIndexTest {

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
}
