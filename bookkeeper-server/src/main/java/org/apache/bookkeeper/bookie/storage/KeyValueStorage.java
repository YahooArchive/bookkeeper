package org.apache.bookkeeper.bookie.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

public interface KeyValueStorage extends Iterable<Entry<byte[], byte[]>>, Closeable {

    void put(byte[] key, byte[] value) throws IOException;

    void put(Iterable<Entry<byte[], byte[]>> entries) throws IOException;

    /**
     * Get the value associated with the given key
     * 
     * @param key
     *            the key to lookup
     * @return the value or null if the key was not found
     */
    byte[] get(byte[] key) throws IOException;

    /**
     * Get the entry whose key is the biggest and it's lesser than the supplied key.
     * <p>
     * For example if the db contains :
     * 
     * <pre>
     * {
     *      1 : 'a',
     *      2 : 'b',
     *      3 : 'c'
     * }
     * </pre>
     * 
     * Then:
     * 
     * <pre>
     * getFloor(3) --> (2, 'b')
     * </pre>
     * 
     * @param key
     *            the non-inclusive upper limit key
     * @return the entry before or null if there's no entry before key
     */
    Entry<byte[], byte[]> getFloor(byte[] key) throws IOException;

    /**
     * 
     * @param key
     * @throws IOException
     */
    void delete(byte[] key) throws IOException;

    void delete(Iterable<byte[]> keys) throws IOException;

    /**
     * Get an iterator over to scan sequentially through all the keys in the database
     * 
     * @return
     */
    Iterable<byte[]> keys();

    /**
     * Get an iterator over to scan sequentially through all the keys within a specified range.
     * 
     * @param firstKey
     *            the first key in the range (included)
     * @param lastKey
     *            the lastKey in the range (not included)
     * 
     */
    Iterable<byte[]> keys(byte[] firstKey, byte[] lastKey);

    /**
     * Return an iterator object that can be used to sequentially scan through all the entries in the database
     */
    @Override
    Iterator<Entry<byte[], byte[]>> iterator();

}
