package org.apache.bookkeeper.bookie.storage.ldb;

import org.apache.bookkeeper.bookie.storage.ldb.LongPair;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class LongPairTest {

    @Test
    public void simple() {
        LongPair l = new LongPair(1, 2);

        assertEquals(1, l.first);
        assertEquals(2, l.second);

        assertEquals(0, l.compareTo(new LongPair(1, 2)));
        assertEquals(+1, l.compareTo(new LongPair(1, 1)));
        assertEquals(+1, l.compareTo(new LongPair(0, 3)));
        assertEquals(-1, l.compareTo(new LongPair(1, 3)));
        assertEquals(-1, l.compareTo(new LongPair(2, 1)));

        assertEquals("(1, 2)", l.toString());

        assertEquals(0, l.compareTo(LongPair.fromArray(l.toArray())));
    }
}
