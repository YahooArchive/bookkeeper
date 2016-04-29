/**
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

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.GarbageCollector.GarbageCleaner;
import org.apache.bookkeeper.bookie.GarbageCollectorThread.CompactableLedgerStorage.EntryLocation;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends SafeRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int SECOND = 1000;

    // Maps entry log files to the set of ledgers that comprise the file and the size usage per ledger
    private Map<Long, EntryLogMetadata> entryLogMetaMap = new ConcurrentHashMap<Long, EntryLogMetadata>();

    ScheduledExecutorService gcExecutor;
    Future<?> scheduledFuture = null;
    // This is how often we want to run the Garbage Collector Thread (in milliseconds).
    final long gcWaitTime;

    // Compaction parameters
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;

    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;

    long lastMinorCompactionTime;
    long lastMajorCompactionTime;

    final int maxOutstandingRequests;
    final int compactionRate;
    final CompactionScannerFactory scannerFactory;

    // Entry Logger Handle
    final EntryLogger entryLogger;
    final LedgerManagerProvider ledgerManagerProvider;

    final ServerConfiguration conf;
    final CompactableLedgerStorage ledgerStorage;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    final GarbageCleaner garbageCleaner;

    boolean ownZk = false;
    boolean enableGcOverReplicatedLedger;
    final long gcOverReplicatedLedgerIntervalMillis;
    long lastOverReplicatedLedgerGcTimeMillis;
    final BookieSocketAddress selfBookieAddress;

    /**
     * Interface that identifies LedgerStorage implementations using EntryLogger and running periodic entries compaction
     */
    public interface CompactableLedgerStorage extends LedgerStorage {

        /**
         * @return the EntryLogger used by the ledger storage
         */
        EntryLogger getEntryLogger();

        /**
         * Get an iterator over a range of ledger ids stored in the bookie.
         *
         * @param firstLedgerId first ledger id in the sequence (included)
         * @param lastLedgerId last ledger id in the sequence (not included)
         * @return
         */
        Iterable<Long> getActiveLedgersInRange(long firstLedgerId, long lastLedgerId)
                throws IOException;

        /**
         * Update the location of several entries and sync the underlying storage
         *
         * @param locations
         *            the list of locations to update
         * @throws IOException
         */
        void updateEntriesLocations(Iterable<EntryLocation> locations) throws IOException;

        public static class EntryLocation {
            public final long ledger;
            public final long entry;
            public final long location;

            public EntryLocation(long ledger, long entry, long location) {
                this.ledger = ledger;
                this.entry = entry;
                this.location = location;
            }
        }
    }

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file
     */
    class CompactionScannerFactory {
        List<EntryLocation> offsets = new ArrayList<EntryLocation>();

        EntryLogScanner newScanner(final EntryLogMetadata meta) {
            final RateLimiter rateLimiter = RateLimiter.create(compactionRate);
            return new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return meta.containsLedger(ledgerId);
                }

                @Override
                public void process(final long ledgerId, long offset, ByteBuffer entry) throws IOException {
                    rateLimiter.acquire();
                    if (offsets.size() > maxOutstandingRequests) {
                        flush();
                    }
                    entry.getLong(); // discard ledger id, we already have it
                    long entryId = entry.getLong();
                    entry.rewind();

                    long newoffset = entryLogger.addEntry(ledgerId, entry);
                    offsets.add(new EntryLocation(ledgerId, entryId, newoffset));

                }
            };
        }

        void flush() throws IOException {
            if (offsets.isEmpty()) {
                LOG.debug("Skipping entry log flushing, as there are no offset!");
                return;
            }

            // Before updating the index, we want to wait until all the compacted entries are flushed into the
            // entryLog
            try {
                entryLogger.flush();

                ledgerStorage.updateEntriesLocations(offsets);
            } finally {
                offsets.clear();
            }
        }
    }


    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf,
                                  LedgerManagerProvider ledgerManagerProvider,
                                  final CompactableLedgerStorage ledgerStorage)
        throws IOException {
        gcExecutor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("GarbageCollectorThread"));

        this.entryLogger = ledgerStorage.getEntryLogger();
        this.ledgerStorage = ledgerStorage;
        this.ledgerManagerProvider = ledgerManagerProvider;
        this.conf = conf;

        this.gcWaitTime = conf.getGcWaitTime();
        this.maxOutstandingRequests = conf.getCompactionMaxOutstandingRequests();
        this.compactionRate = conf.getCompactionRate();
        this.scannerFactory = new CompactionScannerFactory();

        this.garbageCleaner = new GarbageCollector.GarbageCleaner() {
            @Override
            public void clean(long ledgerId) {
                try {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("delete ledger : " + ledgerId);
                    }

                    ledgerStorage.deleteLedger(ledgerId);
                } catch (IOException e) {
                    LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
                }
            }
        };

        selfBookieAddress = Bookie.getBookieAddress(conf);

        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;

        if (minorCompactionInterval > 0 && minorCompactionThreshold > 0) {
            if (minorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid minor compaction threshold "
                                    + minorCompactionThreshold);
            }
            if (minorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short minor compaction interval : "
                                    + minorCompactionInterval);
            }
            enableMinorCompaction = true;
        }

        if (majorCompactionInterval > 0 && majorCompactionThreshold > 0) {
            if (majorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid major compaction threshold "
                                    + majorCompactionThreshold);
            }
            if (majorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short major compaction interval : "
                                    + majorCompactionInterval);
            }
            enableMajorCompaction = true;
        }

        if (enableMinorCompaction && enableMajorCompaction) {
            if (minorCompactionInterval >= majorCompactionInterval ||
                minorCompactionThreshold >= majorCompactionThreshold) {
                throw new IOException("Invalid minor/major compaction settings : minor ("
                                    + minorCompactionThreshold + ", " + minorCompactionInterval
                                    + "), major (" + majorCompactionThreshold + ", "
                                    + majorCompactionInterval + ")");
            }
        }

        gcOverReplicatedLedgerIntervalMillis = conf.getGcOverreplicatedLedgerWaitTimeMillis();
        if (gcOverReplicatedLedgerIntervalMillis > 0) {
            enableGcOverReplicatedLedger = true;
        }

        LOG.info("Minor Compaction : enabled=" + enableMinorCompaction + ", threshold="
               + minorCompactionThreshold + ", interval=" + minorCompactionInterval);
        LOG.info("Major Compaction : enabled=" + enableMajorCompaction + ", threshold="
               + majorCompactionThreshold + ", interval=" + majorCompactionInterval);
        LOG.info("Over Replicated Ledger Deletion : enabled=" + enableGcOverReplicatedLedger + ", interval="
                + gcOverReplicatedLedgerIntervalMillis);

        lastMinorCompactionTime = lastMajorCompactionTime = lastOverReplicatedLedgerGcTimeMillis = MathUtils.now();
    }

    public synchronized void enableForceGC() {
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: {}", Thread.currentThread().getName());
            triggerGC();
        }
    }

    public void disableForceGC() {
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                     .currentThread().getName());
        }
    }

    /**
     * Manually trigger GC (for testing)
     */
    Future<?> triggerGC() {
        return gcExecutor.submit(this);
    }

    public synchronized void start() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        scheduledFuture = gcExecutor.scheduleAtFixedRate(this, gcWaitTime, gcWaitTime, TimeUnit.MILLISECONDS);
    }

    @Override
    public void safeRun() {
        boolean force = forceGarbageCollection.get();
        if (force) {
            LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
        }

        // Extract all of the ledger ID's that comprise all of the entry logs
        // (except for the current new one which is still being written to).
        entryLogMetaMap = extractMetaFromEntryLogs(entryLogMetaMap);
        ZooKeeper zk = null;

        try {
            long curTime = MathUtils.now();
            LedgerManager ledgerManager = ledgerManagerProvider.getLedgerManager();
            boolean checkOverreplicatedLedgers = (enableGcOverReplicatedLedger && curTime
                    - lastOverReplicatedLedgerGcTimeMillis > gcOverReplicatedLedgerIntervalMillis);
            if (checkOverreplicatedLedgers) {
                if (ledgerManagerProvider instanceof LedgerManagerProviderImpl) {
                    zk = ((LedgerManagerProviderImpl) ledgerManagerProvider).getZooKeeper();
                } else {
                    zk = ZkUtils.createConnectedZookeeperClient(conf.getZkServers(),
                            new ZooKeeperWatcherBase(conf.getZkTimeout()));
                    ownZk = true;
                }
            }
            // gc inactive/deleted ledgers
            GarbageCollector collector = new ScanAndCompareGarbageCollector(ledgerManager, ledgerStorage,
                    selfBookieAddress, zk, checkOverreplicatedLedgers, conf.getZkLedgersRootPath());

            collector.gc(garbageCleaner);

            if (checkOverreplicatedLedgers) {
                lastOverReplicatedLedgerGcTimeMillis = MathUtils.now();
            }

            // gc entry logs
            doGcEntryLogs();

            if (force || (enableMajorCompaction &&
                          curTime - lastMajorCompactionTime > majorCompactionInterval)) {
                // enter major compaction
                LOG.info("Enter major compaction");
                doCompactEntryLogs(majorCompactionThreshold);
                lastMajorCompactionTime = MathUtils.now();
                // also move minor compaction time
                lastMinorCompactionTime = lastMajorCompactionTime;
            }

            if (force || (enableMinorCompaction &&
                          curTime - lastMinorCompactionTime > minorCompactionInterval)) {
                // enter minor compaction
                LOG.info("Enter minor compaction");
                doCompactEntryLogs(minorCompactionThreshold);
                lastMinorCompactionTime = MathUtils.now();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.info("Garbage collection interrupted", ie);
        } catch (Exception e) {
            LOG.warn("Exception in gc", e);
        } finally {
            try {
                ledgerManagerProvider.releaseResources();
                if (ownZk && zk != null) {
                    zk.close();
                    ownZk = false;
                }
            } catch (IOException ioe) {
                LOG.warn("Error cleaning up ledger manager resources", ioe);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted cleaning up ledger manager resources", ie);
            }
        }
        forceGarbageCollection.set(false);
    }


    /**
     * Garbage collect those entry loggers which are not associated with any active ledgers
     */
    private void doGcEntryLogs() {
        // Loop through all of the entry logs and remove the non-active ledgers.
        for (Long entryLogId : entryLogMetaMap.keySet()) {
            EntryLogMetadata meta = entryLogMetaMap.get(entryLogId);
            for (Long entryLogLedger : meta.getLedgersMap().keySet()) {
                // Remove the entry log ledger from the set if it isn't active.
                try {
                    if (!ledgerStorage.ledgerExists(entryLogLedger)) {
                        meta.removeLedger(entryLogLedger);
                    }
                } catch (IOException e) {
                    LOG.error("Error reading from ledger storage", e);
                }
            }
            if (meta.isEmpty()) {
                // This means the entry log is not associated with any active ledgers anymore.
                // We can remove this entry log file now.
                LOG.info("Deleting entryLogId " + entryLogId + " as it has no active ledgers!");
                removeEntryLog(entryLogId);
            }
        }
    }

    /**
     * Compact entry logs if necessary.
     *
     * <p>
     * Compaction will be executed from low unused space to high unused space.
     * Those entry log files whose remaining size percentage is higher than threshold
     * would not be compacted.
     * </p>
     */
    @VisibleForTesting
    void doCompactEntryLogs(double threshold) {
        LOG.info("Do compaction to compact those files lower than " + threshold);
        // sort the ledger meta by occupied unused space
        Comparator<EntryLogMetadata> sizeComparator = new Comparator<EntryLogMetadata>() {
            @Override
            public int compare(EntryLogMetadata m1, EntryLogMetadata m2) {
                long unusedSize1 = m1.getTotalSize() - m1.getRemainingSize();
                long unusedSize2 = m2.getTotalSize() - m2.getRemainingSize();
                if (unusedSize1 > unusedSize2) {
                    return -1;
                } else if (unusedSize1 < unusedSize2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        List<EntryLogMetadata> logsToCompact = new ArrayList<EntryLogMetadata>();
        logsToCompact.addAll(entryLogMetaMap.values());
        Collections.sort(logsToCompact, sizeComparator);

        for (EntryLogMetadata meta : logsToCompact) {
            if (meta.getUsage() >= threshold) {
                break;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Compacting entry log {} below threshold {}", meta.getEntryLogId(), threshold);
            }
            try {
                compactEntryLog(scannerFactory, meta);
                scannerFactory.flush();

                LOG.info("Removing entry log {} after compaction", meta.getEntryLogId());
                removeEntryLog(meta.getEntryLogId());

            } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
                LOG.warn("No writable ledger directory available, aborting compaction", nwlde);
                break;
            } catch (IOException ioe) {
                // if compact entry log throws IOException, we don't want to remove that
                // entry log. however, if some entries from that log have been readded
                // to the entry log, and the offset updated, it's ok to flush that
                LOG.error("Error compacting entry log. Log won't be deleted", ioe);
            }

            if (!running) { // if gc thread is not running, stop compaction
                return;
            }
        }
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    public synchronized void shutdown() throws InterruptedException {
        this.running = false;
        LOG.info("Shutting down GarbageCollectorThread");

        while (!compacting.compareAndSet(false, true)) {
            wait(1000);
        }
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        gcExecutor.shutdown();

        if (gcExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
            LOG.warn("GC executor did not shut down in 60 seconds. Killing");
            gcExecutor.shutdownNow();
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    private void removeEntryLog(long entryLogId) {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            entryLogMetaMap.remove(entryLogId);
        }
    }

    /**
     * Compact an entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected void compactEntryLog(CompactionScannerFactory scannerFactory,
                                   EntryLogMetadata entryLogMeta) throws IOException {
        // Similar with Sync Thread
        // try to mark compacting flag to make sure it would not be interrupted
        // by shutdown during compaction. otherwise it will receive
        // ClosedByInterruptException which may cause index file & entry logger
        // closed and corrupted.
        if (!compacting.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates another thread wants to interrupt gc thread to exit
            return;
        }

        LOG.info("Compacting entry log : {} - Usage: {} %", entryLogMeta.getEntryLogId(), entryLogMeta.getUsage());

        try {
            entryLogger.scanEntryLog(entryLogMeta.getEntryLogId(),
                                     scannerFactory.newScanner(entryLogMeta));
        } finally {
            // clear compacting flag
            compacting.set(false);
        }
    }

    /**
     * Method to read in all of the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @param entryLogMetaMap
     *          Existing EntryLogs to Meta
     * @throws IOException
     */
    protected Map<Long, EntryLogMetadata> extractMetaFromEntryLogs(Map<Long, EntryLogMetadata> entryLogMetaMap) {
        // Extract it for every entry log except for the current one.
        // Entry Log ID's are just a long value that starts at 0 and increments
        // by 1 when the log fills up and we roll to a new one.
        long curLogId = entryLogger.getLeastUnflushedLogId();
        boolean hasExceptionWhenScan = false;
        for (long entryLogId = scannedLogId; entryLogId < curLogId; entryLogId++) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetaMap.containsKey(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: {}", entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = entryLogger.getEntryLogMetadata(entryLogId);
                entryLogMetaMap.put(entryLogId, entryLogMeta);
            } catch (IOException e) {
                hasExceptionWhenScan = true;
                LOG.warn("Premature exception when processing " + entryLogId +
                         " recovery will take care of the problem", e);
            }

            // if scan failed on some entry log, we don't move 'scannedLogId' to next id
            // if scan succeed, we don't need to scan it again during next gc run,
            // we move 'scannedLogId' to next id
            if (!hasExceptionWhenScan) {
                ++scannedLogId;
            }
        }
        return entryLogMetaMap;
    }

    /**
     * This interfaces exist so dummy ledger managers can
     * be injected for testing. We should move to a proper DI implementation at some point.
     */
    public interface LedgerManagerProvider {
        LedgerManager getLedgerManager() throws InterruptedException, KeeperException, IOException;
        void releaseResources() throws IOException, InterruptedException;
    }

    public static class LedgerManagerProviderImpl implements LedgerManagerProvider {
        final ServerConfiguration conf;
        ZooKeeper zk = null;
        LedgerManagerFactory lmfactory = null;
        LedgerManager ledgerManager = null;

        LedgerManagerProviderImpl(ServerConfiguration conf) {
            this.conf = conf;
        }

        public LedgerManager getLedgerManager() throws InterruptedException, KeeperException, IOException {
            zk = ZkUtils.createConnectedZookeeperClient(conf.getZkServers(),
                                                        new ZooKeeperWatcherBase(conf.getZkTimeout()));
            lmfactory = LedgerManagerFactory.newLedgerManagerFactory(conf, zk);
            LOG.info("instantiate ledger manager {}", lmfactory.getClass().getName());
            ledgerManager = lmfactory.newLedgerManager();
            return ledgerManager;
        }

        public ZooKeeper getZooKeeper() {
            return zk;
        }

        public void releaseResources() throws IOException, InterruptedException {
            if (ledgerManager != null) {
                ledgerManager.close();
                ledgerManager = null;
            }
            if (lmfactory != null) {
                lmfactory.uninitialize();
                lmfactory = null;
            }
            if (zk != null) {
                zk.close();
                zk = null;
            }
        }

    }
}
