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

import java.util.Set;

import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Garbage collector implementation using scan and compare.
 *
 * <p>
 * Garbage collection is processed as below:
 * <ul>
 * <li> fetch all existing ledgers from zookeeper or metastore according to
 * the LedgerManager, called <b>globalActiveLedgers</b>
 * <li> fetch all active ledgers from bookie server, said <b>bkActiveLedgers</b>
 * <li> loop over <b>bkActiveLedgers</b> to find those ledgers that are not in
 * <b>globalActiveLedgers</b>, do garbage collection on them.
 * </ul>
 * </p>
 */
public class ScanAndCompareGarbageCollector implements GarbageCollector{

    static final Logger LOG = LoggerFactory.getLogger(ScanAndCompareGarbageCollector.class);
    private final LedgerManager ledgerManager;
    private final LedgerStorage ledgerStorage;

    public ScanAndCompareGarbageCollector(LedgerManager ledgerManager, LedgerStorage ledgerStorage) {
        this.ledgerManager = ledgerManager;
        this.ledgerStorage = ledgerStorage;
    }

    @Override
    public void gc(GarbageCleaner garbageCleaner) {
        LedgerRangeIterator ledgerRangeIterator = ledgerManager.getLedgerRanges();
        try {
            // Empty global active ledgers, need to remove all local active ledgers.
            if (!ledgerRangeIterator.hasNext()) {
                ledgerStorage.deleteAllLedgers();
            }
            long lastEnd = 0;

            while(ledgerRangeIterator.hasNext()) {
                LedgerRange lRange = ledgerRangeIterator.next();

                Long start = lastEnd;
                Long end = lRange.end() + 1;
                if (!ledgerRangeIterator.hasNext()) {
                    end = Long.MAX_VALUE;
                }

                Iterable<Long> subBkActiveLedgers = ledgerStorage.getActiveLedgersInRange(start, end);

                Set<Long> ledgersInMetadata = lRange.getLedgers();
                LOG.debug("Active in metadata {}, Active in bookie {}",
                          ledgersInMetadata, subBkActiveLedgers);
                for (Long bkLid : subBkActiveLedgers) {
                    if (!ledgersInMetadata.contains(bkLid)) {
                        ledgerStorage.deleteLedger(bkLid);
                        garbageCleaner.clean(bkLid);
                    }
                }
                lastEnd = end;
            }
        } catch (Exception e) {
            // ignore exception, collecting garbage next time
            LOG.warn("Exception when iterating over the metadata {}", e);
        }
    }
}


