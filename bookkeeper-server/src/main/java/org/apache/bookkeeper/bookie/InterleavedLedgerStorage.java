/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SnapshotMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
public class InterleavedLedgerStorage implements CompactableLedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    EntryLogger entryLogger;
    LedgerCache ledgerCache;

    // A sorted map to stored all active ledger ids
    protected final SnapshotMap<Long, Boolean> activeLedgers;

    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    GarbageCollectorThread gcThread;

    public InterleavedLedgerStorage() {
        activeLedgers = new SnapshotMap<Long, Boolean>();
    }

    @Override
    public void initialize(ServerConfiguration conf,
                           GarbageCollectorThread.LedgerManagerProvider ledgerManagerProvider,
                           LedgerDirsManager ledgerDirsManager, StatsLogger stats)
            throws IOException {
        entryLogger = new EntryLogger(conf, ledgerDirsManager);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgers, ledgerDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerManagerProvider, this);
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        gcThread.shutdown();
        entryLogger.shutdown();
        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return ledgerCache.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return ledgerCache.isFenced(ledgerId);
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    synchronized public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        
        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry);
        
        
        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        long offset;
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        if (offset == 0) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return ByteBuffer.wrap(entryLogger.readEntry(ledgerId, entryId, offset));
    }
    
    @Override
    public void trimEntries(long ledgerId, long lastEntryId) throws IOException {
        // Ignore trimming request
        LOG.debug("TrimEntries: {}@{}", ledgerId, lastEntryId);
    }

    @Override
    public boolean isFlushRequired() {
        return entryLogger.isFlushRequired();
    }

    @Override
    public void flush() throws IOException {
        if (!isFlushRequired()) {
            return;
        }
        boolean flushFailed = false;

        try {
            ledgerCache.flushLedger(true);
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }

        try {
            entryLogger.flush();
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        activeLedgers.remove(ledgerId);
        ledgerCache.deleteLedger(ledgerId);
    }

    @Override
    public Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId) {
        NavigableMap<Long, Boolean> bkActiveLedgersSnapshot = activeLedgers.snapshot();
        Map<Long, Boolean> subBkActiveLedgers = bkActiveLedgersSnapshot
                .subMap(firstLedgerId, true, lastLedgerId, false);

        return subBkActiveLedgers.keySet();
    }

    @Override
    public void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException {
        for (EntryLocation l : locations) {
            ledgerCache.putEntryOffset(l.ledger, l.entry, l.location);
        }

        ledgerCache.flushLedger(true);
    }

    @Override
    public EntryLogger getEntryLogger() {
        return entryLogger;
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return ledgerCache.getJMXBean();
    }
}
