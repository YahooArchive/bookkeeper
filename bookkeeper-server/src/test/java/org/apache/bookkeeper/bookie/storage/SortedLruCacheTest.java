package org.apache.bookkeeper.bookie.storage;

import org.junit.Test;
import static junit.framework.Assert.assertEquals;

public class SortedLruCacheTest {

    @Test
    public void simple() {
        SortedLruCache<Integer, Integer> cache = new SortedLruCache<Integer, Integer>(10);

        cache.put(1, 1);
        cache.put(2, 2);

        assertEquals(2, cache.getSize());
        assertEquals(2, cache.getNumberOfEntries());

        assertEquals(new Integer(1), cache.get(1));
        assertEquals(new Integer(2), cache.get(2));
        assertEquals(null, cache.get(3));

        assertEquals(2, cache.getSize());
        assertEquals(2, cache.getNumberOfEntries());

        cache.put(3, 3);
        cache.put(4, 4);
        cache.put(5, 5);
        cache.put(6, 6);
        cache.put(7, 7);
        cache.put(8, 8);
        cache.put(9, 9);
        cache.put(10, 10);

        assertEquals(10, cache.getSize());
        assertEquals(10, cache.getNumberOfEntries());
        assertEquals(new Integer(1), cache.get(1));
        assertEquals(new Integer(2), cache.get(2));
        assertEquals(new Integer(6), cache.get(6));

        assertEquals(10, cache.getSize());
        assertEquals(10, cache.getNumberOfEntries());

        cache.put(11, 11);

        assertEquals(10, cache.getSize());

        assertEquals(null, cache.get(3));

        assertEquals(10, cache.getSize());
        assertEquals(10, cache.getNumberOfEntries());

        cache.clear();
        assertEquals(0, cache.getSize());
        assertEquals(0, cache.getNumberOfEntries());

        cache.put(1, 1);
        cache.put(1, 2);
        assertEquals(1, cache.getSize());
        assertEquals(1, cache.getNumberOfEntries());

        assertEquals(new Integer(2), cache.get(1));
    }
}
