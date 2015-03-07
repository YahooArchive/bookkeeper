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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

class HandleFactoryImpl implements HandleFactory {
    private final Cache<Long, LedgerDescriptor> ledgers;
    private final Cache<Long, LedgerDescriptor> readOnlyLedgers;

    final LedgerStorage ledgerStorage;

    HandleFactoryImpl(LedgerStorage ledgerStorage) {
        this.ledgerStorage = ledgerStorage;
        this.ledgers = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();
        this.readOnlyLedgers = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();
    }

    @Override
    public LedgerDescriptor getHandle(final long ledgerId, final byte[] masterKey) throws IOException, BookieException {
        try {
            LedgerDescriptor handle = ledgers.get(ledgerId, new Callable<LedgerDescriptor>() {
                @Override
                public LedgerDescriptor call() throws Exception {
                    return LedgerDescriptor.create(masterKey, ledgerId, ledgerStorage);
                }
            });

            handle.checkAccess(masterKey);
            return handle;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e.getCause());
            }
        }
    }

    @Override
    public LedgerDescriptor getReadOnlyHandle(final long ledgerId) throws IOException, Bookie.NoLedgerException {
        try {
            return readOnlyLedgers.get(ledgerId, new Callable<LedgerDescriptor>() {
                @Override
                public LedgerDescriptor call() throws Exception {
                    return LedgerDescriptor.createReadOnly(ledgerId, ledgerStorage);
                }
            });
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            } else {
                throw new IOException(e.getCause());
            }
        }
    }
}
