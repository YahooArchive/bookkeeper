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

package org.apache.bookkeeper.bookie.storage.ldb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.storage.ldb.KeyValueStorage.Batch;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Scan all entries in the entry log and rebuild the ledgerStorageIndex
 */
public class LocationsIndexRebuildOp {
    private final ServerConfiguration conf;

    public LocationsIndexRebuildOp(ServerConfiguration conf) {
        this.conf = conf;
    }

    public void initiate() throws IOException {
        LOG.info("Starting index rebuilding");

        // Move locations index to a backup directory
        String basePath = Bookie.getCurrentDirectory(conf.getLedgerDirs()[0]).toString();
        Path currentPath = FileSystems.getDefault().getPath(basePath, "locations");
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date());
        Path backupPath = FileSystems.getDefault().getPath(basePath, "locations.BACKUP-" + timestamp);
        Files.move(currentPath, backupPath);

        LOG.info("Created locations index backup at {}", backupPath);

        long startTime = System.nanoTime();

        EntryLogger entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs()));
        Set<Long> entryLogs = entryLogger.getEntryLogsSet();

        boolean rocksDBEnabled = conf.getBoolean(DbLedgerStorage.ROCKSDB_ENABLED, false);
        KeyValueStorageFactory storageFactory = rocksDBEnabled ? //
                KeyValueStorageRocksDB.factory //
                : KeyValueStorageLevelDB.factory;
        String locationsDbPath = FileSystems.getDefault().getPath(basePath, "locations").toFile().toString();

        Set<Long> activeLedgers = getActiveLedgers(storageFactory, basePath);
        LOG.info("Found {} active ledgers in ledger manager", activeLedgers.size());

        KeyValueStorage newIndex = storageFactory.newKeyValueStorage(locationsDbPath);

        int totalEntryLogs = entryLogs.size();
        int completedEntryLogs = 0;
        LOG.info("Scanning {} entry logs", totalEntryLogs);

        Map<LongPair, LedgerIndexPage> pages = Maps.newHashMap();

        for (long entryLogId : entryLogs) {
            entryLogger.scanEntryLog(entryLogId, new EntryLogScanner() {
                @Override
                public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException {
                    entry.getLong(); // discard ledger id, we already have it
                    long entryId = entry.getLong();
                    entry.rewind();

                    // Actual location indexed is pointing past the entry size
                    long location = offset + 4;

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Rebuilding {}:{} at location {} / {}",
                                new Object[] { ledgerId, entryId, location >> 32, location & (Integer.MAX_VALUE - 1) });
                    }

                    // Update the ledger index page
                    LongPair pageStart = getPageStart(ledgerId, entryId);
                    LedgerIndexPage page = pages.get(pageStart);
                    if (page == null) {
                        // We don't have the page cached, we'll try to read from db or create a new one
                        byte[] key = pageStart.toArray();
                        byte[] value = newIndex.get(key);
                        if (value != null) {
                            // Page was in db already
                            page = new LedgerIndexPage(key, value);
                        } else {
                            // Create a new page aligned on 1000 entries
                            page = new LedgerIndexPage(key, new byte[1000 * 8]);
                        }

                        pages.put(pageStart, page);
                    }

                    page.setPosition(entryId, location);
                }

                @Override
                public boolean accept(long ledgerId) {
                    return activeLedgers.contains(ledgerId);
                }
            });

            // Done scanning one entryLog, updating index
            Batch batch = newIndex.newBatch();
            for (LedgerIndexPage page : pages.values()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Reindexing page: {}", page);
                }
                batch.put(page.getKey(), page.getValue());
            }
            batch.flush();

            ++completedEntryLogs;
            LOG.info("Completed scanning of log {}.log -- {} / {}",
                    new Object[] { Long.toHexString(entryLogId), completedEntryLogs, totalEntryLogs });

            pages.clear();
        }

        newIndex.close();

        LOG.info("Rebuilding index is done. Total time: {}",
                DurationFormatUtils.formatDurationHMS(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)));
    }

    private static LongPair getPageStart(long ledgerId, long entryId) {
        long alignedEntryId = entryId - (entryId % 1000);
        return new LongPair(ledgerId, alignedEntryId);
    }

    private Set<Long> getActiveLedgers(KeyValueStorageFactory storageFactory, String basePath) throws IOException {
        LedgerMetadataIndex ledgers = new LedgerMetadataIndex(storageFactory, basePath, NullStatsLogger.INSTANCE);
        Set<Long> activeLedgers = Sets.newHashSet();
        for (Long ledger : ledgers.getActiveLedgersInRange(0, Long.MAX_VALUE)) {
            activeLedgers.add(ledger);
        }

        ledgers.close();
        return activeLedgers;
    }

    private static final Logger LOG = LoggerFactory.getLogger(LocationsIndexRebuildOp.class);
}
