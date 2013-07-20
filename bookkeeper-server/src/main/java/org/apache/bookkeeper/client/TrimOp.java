/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.util.List;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrimOp {
    LedgerHandle lh;
    long lastEntryId;

    TrimOp(LedgerHandle lh, long lastEntryId) {
        this.lh = lh;
        this.lastEntryId = lastEntryId;
    }

    void initiate() {
        log.debug("Initiating trimming of {}@{}", lastEntryId, lh.getId());
        if (lastEntryId < 0) {
            // No need to send the trim request
            return;
        }

        List<InetSocketAddress> bookies = lh.metadata.currentEnsemble;
        int flags = BookieProtocol.FLAG_NONE;
        for (InetSocketAddress bookie : bookies) {
            lh.bk.bookieClient.trim(bookie, lh.ledgerId, lh.ledgerKey, lastEntryId, flags);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(TrimOp.class);
}
