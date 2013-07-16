package org.apache.bookkeeper.proto;

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

import org.junit.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.PerChannelBookieClient.ConnectionState;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;

/**
 * Tests for PerChannelBookieClient. Historically, this class has
 * had a few race conditions, so this is what these tests focus on.
 */
public class TestPerChannelBookieClient extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);

    public TestPerChannelBookieClient() {
        super(1);
    }

    /**
     * Test that a race does not exist between connection completion
     * and client closure. If a race does exist, this test will simply
     * hang at releaseExternalResources() as it is uninterruptible.
     * This specific race was found in
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-485}.
     */
    @Test(timeout=60000)
    public void testConnectCloseRace() throws Exception {
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        
        ClientConfiguration conf = newClientConfiguration();
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ClientAuthProvider.Factory authFactory = AuthProviderFactoryFactory
                .newClientAuthProviderFactory(conf, registry);

        InetSocketAddress addr = getBookie(0);
        AtomicLong bytesOutstanding = new AtomicLong(0);
        for (int i = 0; i < 1000; i++) {
            PerChannelBookieClient client = new PerChannelBookieClient(conf, executor, channelFactory, addr,
                    bytesOutstanding, authFactory, registry);
            client.connectIfNeededAndDoOp(new GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        // do nothing, we don't care about doing anything with the connection,
                        // we just want to trigger it connecting.
                    }
                });
            client.close();
        }
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    /**
     * Test race scenario found in {@link https://issues.apache.org/jira/browse/BOOKKEEPER-5}
     * where multiple clients try to connect a channel simultaneously. If not synchronised
     * correctly, this causes the netty channel to get orphaned.
     */
    @Test(timeout=60000)
    public void testConnectRace() throws Exception {
        GenericCallback<Void> nullop = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                // do nothing, we don't care about doing anything with the connection,
                // we just want to trigger it connecting.
            }
        };
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);

        ClientConfiguration conf = newClientConfiguration();
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ClientAuthProvider.Factory authFactory = AuthProviderFactoryFactory
                .newClientAuthProviderFactory(conf, registry);

        InetSocketAddress addr = getBookie(0);
        AtomicLong bytesOutstanding = new AtomicLong(0);
        for (int i = 0; i < 100; i++) {
            PerChannelBookieClient client = new PerChannelBookieClient(conf, executor, channelFactory, addr,
                    bytesOutstanding, authFactory, registry);
            for (int j = i; j < 10; j++) {
                client.connectIfNeededAndDoOp(nullop);
            }
            client.close();
        }
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    /**
     * Test that all resources are freed if connections and disconnections
     * are interleaved randomly.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-620}
     */
    @Test(timeout=60000)
    public void testDisconnectRace() throws Exception {
        final GenericCallback<Void> nullop = new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                // do nothing, we don't care about doing anything with the connection,
                // we just want to trigger it connecting.
            }
        };
        final int ITERATIONS = 100000;
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        InetSocketAddress addr = getBookie(0);
        
        ClientConfiguration conf = newClientConfiguration();
        ExtensionRegistry registry = ExtensionRegistry.newInstance();
        ClientAuthProvider.Factory authFactory = AuthProviderFactoryFactory
                .newClientAuthProviderFactory(conf, registry);

        AtomicLong bytesOutstanding = new AtomicLong(0);
        final PerChannelBookieClient client = new PerChannelBookieClient(conf, executor, channelFactory, addr,
                bytesOutstanding, authFactory, registry);
        final AtomicBoolean shouldFail = new AtomicBoolean(false);
        final AtomicBoolean running = new AtomicBoolean(true);
        final CountDownLatch disconnectRunning = new CountDownLatch(1);
        Thread connectThread = new Thread() {
                public void run() {
                    try {
                        if (!disconnectRunning.await(10, TimeUnit.SECONDS)) {
                            LOG.error("Disconnect thread never started");
                            shouldFail.set(true);
                        }
                    } catch (InterruptedException ie) {
                        LOG.error("Connect thread interrupted", ie);
                        Thread.currentThread().interrupt();
                        running.set(false);
                    }
                    for (int i = 0; i < ITERATIONS && running.get(); i++) {
                        client.connectIfNeededAndDoOp(nullop);
                    }
                    running.set(false);
                }
            };
        Thread disconnectThread = new Thread() {
                public void run() {
                    disconnectRunning.countDown();
                    while (running.get()) {
                        client.disconnect();
                    }
                }
            };
        Thread checkThread = new Thread() {
                public void run() {
                    ConnectionState state;
                    Channel channel;
                    while (running.get()) {
                        synchronized (client) {
                            state = client.state;
                            channel = client.channel;

                            if ((state == ConnectionState.CONNECTED
                                 && (channel == null
                                     || !channel.isConnected()))
                                || (state != ConnectionState.CONNECTED
                                    && channel != null
                                    && channel.isConnected())) {
                                LOG.error("State({}) and channel({}) inconsistent " + channel,
                                          state, channel == null ? null : channel.isConnected());
                                shouldFail.set(true);
                                running.set(false);
                            }
                        }
                    }
                }
            };
        connectThread.start();
        disconnectThread.start();
        checkThread.start();

        connectThread.join();
        disconnectThread.join();
        checkThread.join();
        assertFalse("Failure in threads, check logs", shouldFail.get());

        client.close();
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }
}
