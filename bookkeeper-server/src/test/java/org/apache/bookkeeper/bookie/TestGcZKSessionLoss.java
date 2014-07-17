package org.apache.bookkeeper.bookie;

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

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Before;
import org.junit.Test;

/**
 * Test bookie gc behaviour when zookeeper session is lost
 */
public class TestGcZKSessionLoss extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(CompactionTest.class);

    public TestGcZKSessionLoss() {
        super(1);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setZkTimeout(1000);

        super.setUp();
    }

    @Test(timeout=10000)
    public void testZKSessionLoss() throws Exception {
        long l1 = createAndWriteLedger();
        long l2 = createAndWriteLedger();
        long l3 = createAndWriteLedger();
        long l4 = createAndWriteLedger();
        long l5 = createAndWriteLedger();

        Bookie b = bs.get(0).getBookie();

        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l2));
        bkc.deleteLedger(l2);

        BookieAccessor.triggerGC(b).get();
        assertFalse("ledger shouldn't exist", BookieAccessor.ledgerExists(b, l2));
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l1));
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l3));

        bkc.deleteLedger(l4);
        zkUtil.shutdownDaemon();
        BookieAccessor.triggerGC(b).get();
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l3));
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l4));
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l5));

        zkUtil.startDaemon();
        BookieAccessor.triggerGC(b).get();
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l3));
        assertFalse("ledger shouldn't exist", BookieAccessor.ledgerExists(b, l4));
        assertTrue("ledger should exist", BookieAccessor.ledgerExists(b, l5));
    }

    long createAndWriteLedger() throws Exception {
        LedgerHandle lh = bkc.createLedger(1, 1, 1,
                DigestType.MAC, "foobar".getBytes());
        for (int i = 0; i < 5; i++) {
            lh.addEntry("Foobar".getBytes());
        }
        lh.close();
        return lh.getId();
    }
}



