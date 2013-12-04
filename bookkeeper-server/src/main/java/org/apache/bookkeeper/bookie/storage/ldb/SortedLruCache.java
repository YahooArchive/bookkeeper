/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie.storage.ldb;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.Lists;

/**
 * Special type of LRU where get() and delete() operations can be done over a range of keys.
 * <p>
 * The LRU eviction is implemented by having a linked list and pushing entries to the head of the list during access.
 * 
 * @param <Key>
 *            Cache key. Needs to be Comparable
 * @param <Value>
 *            Cache value
 */
public class SortedLruCache<Key extends Comparable<Key>, Value> {
    // Map from key to nodes inside the linked list
    private final NavigableMap<Key, Node<Key, Value>> entries;
    private Node<Key, Value> head; // Head of the linked list
    private Node<Key, Value> tail; // Tail of the linked list
    private long size; // Total size of values stored in cache
    private final Weighter<Value> weighter; // Weighter object used to extract the size from values

    private final long maxSize;

    /**
     * Construct a new RangeLruCache with default Weighter
     */
    public SortedLruCache(long maxSize) {
        this(maxSize, new DefaultWeighter<Value>());
    }

    /**
     * Construct a new RangeLruCache
     * 
     * @param weighter
     *            a custom weighter to compute the size of each stored value
     */
    public SortedLruCache(long maxSize, Weighter<Value> weighter) {
        this.maxSize = maxSize;
        this.size = 0;
        this.entries = new TreeMap<Key, Node<Key, Value>>();
        this.weighter = weighter;
        this.head = null;
        this.tail = null;
    }

    /**
     * Insert a new entry in the cache.
     * 
     * Triggers an eviction if the cache is full.
     * 
     * @param key
     * @param value
     * @return
     */
    public synchronized Value put(Key key, Value value) {
        Node<Key, Value> newNode = new Node<Key, Value>(key, value);
        Node<Key, Value> oldNode = entries.put(key, newNode);

        // Add the new entry at the beginning of the list
        addFirst(newNode);
        size += weighter.getSize(newNode.value);

        Value oldValue = null;

        if (oldNode != null) {
            // If there were a older value for the same key, we need to remove it and adjust the cache size
            removeFromList(oldNode);
            size -= weighter.getSize(oldNode.value);
            oldValue = oldNode.value;
        }

        if (size > maxSize) {
            evictLeastAccessedEntries();
        }

        return oldValue;
    }

    public synchronized Value get(Key key) {
        Node<Key, Value> node = entries.get(key);
        if (node == null) {
            return null;
        } else {
            moveToHead(node);
            return node.value;
        }
    }

    public synchronized void removeRange(Key start, Key end) {
        List<Key> keysToRemove = Lists.newArrayList(entries.subMap(start, end).keySet());
        for (Key key : keysToRemove) {
            Node<Key, Value> node = entries.get(key);
            entries.remove(key);
            removeFromList(node);
            size -= weighter.getSize(node.value);
        }
    }

    /**
     * 
     * @param minSize
     * @return a pair containing the number of entries evicted and their total size
     */
    private void evictLeastAccessedEntries() {
        while (tail != null && size > maxSize) {
            Node<Key, Value> node = tail;
            entries.remove(node.key);
            removeFromList(node);

            size -= weighter.getSize(node.value);
        }
    }

    public synchronized long getNumberOfEntries() {
        return entries.size();
    }

    public synchronized long getSize() {
        return size;
    }

    /**
     * Remove all the entries from the cache
     * 
     * @return the old size
     */
    public synchronized void clear() {
        entries.clear();
        head = tail = null;
        size = 0;
    }

    /**
     * Interface of a object that is able to the extract the "weight" (size/cost/space) of the cached values
     * 
     * @param <Value>
     */
    public static interface Weighter<Value> {
        long getSize(Value value);
    }

    /**
     * Default cache weighter, every value is assumed the same cost
     * 
     * @param <Value>
     */
    private static class DefaultWeighter<Value> implements Weighter<Value> {
        public long getSize(Value value) {
            return 1;
        }
    }

    // // Private helpers

    /**
     * Remove a node from the linked list
     * 
     * @param node
     */
    private void removeFromList(Node<Key, Value> node) {
        if (node.prev == null) {
            head = node.next;
        } else {
            node.prev.next = node.next;
        }

        if (node.next == null) {
            tail = node.prev;
        } else {
            node.next.prev = node.prev;
        }

        node.next = null;
        node.prev = null;
    }

    /**
     * Insert the node in the first position of the list
     * 
     * @param node
     */
    private void addFirst(Node<Key, Value> node) {
        if (head == null) {
            head = tail = node;
        } else {
            head.prev = node;
            node.next = head;
            head = node;
        }
    }

    /**
     * Move a node to the head of the list
     * 
     * @param node
     */
    private void moveToHead(Node<Key, Value> node) {
        removeFromList(node);
        addFirst(node);
    }

    /**
     * Data structure to hold a linked-list node
     * 
     * @param <Key>
     * @param <Value>
     */
    private static class Node<Key, Value> {
        final Key key;
        final Value value;
        Node<Key, Value> prev;
        Node<Key, Value> next;

        Node(Key key, Value value) {
            this.key = key;
            this.value = value;
            this.prev = null;
            this.next = null;
        }
    }

}
