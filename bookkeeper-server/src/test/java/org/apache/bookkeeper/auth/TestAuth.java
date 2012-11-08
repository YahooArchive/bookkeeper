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
package org.apache.bookkeeper.auth;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.DataFormats.AuthMessage;
import org.apache.bookkeeper.proto.TestDataFormats;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ExtensionRegistry;

public class TestAuth extends BookKeeperClusterTestCase {
    static final Logger LOG = LoggerFactory.getLogger(TestAuth.class);
    private static final String TEST_AUTH_PROVIDER_PLUGIN_NAME = "TestAuthProviderPlugin";

    public TestAuth() {
        super(0); // start them later when auth providers are configured
    }

    private void connectAndWriteToBookie(ClientConfiguration conf)
            throws BKException, Exception {
        LOG.info("Connecting to bookie");
        BookKeeper bkc = new BookKeeper(conf, zkc);
        LedgerHandle l = bkc.createLedger(1, 1, DigestType.CRC32,
                                          "testPasswd".getBytes());
        l.addEntry("TestEntry".getBytes());
        l.close();
        bkc.close();
    }

    /**
     * Test an connection will authorize with a single message
     * to the server and a single response.
     */
    @Test(timeout=30000)
    public void testSingleMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysSucceedBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startBookie(bookieConf);
        connectAndWriteToBookie(clientConf); // should succeed
    }
    
    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write
     */
    @Test(timeout=30000)
    public void testSingleMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                AlwaysFailBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startBookie(bookieConf);
        try {
            connectAndWriteToBookie(clientConf); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // client shouldnt be able to find enough bookies to
            // write
        }
    }

    /**
     * Test that authentication works when the providers
     * exchange multiple messages
     */
    @Test(timeout=30000)
    public void testMultiMessageAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                SucceedAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startBookie(bookieConf);
        connectAndWriteToBookie(clientConf); // should succeed
    }
    
    /**
     * Test that when the bookie provider sends a failure message
     * the client will not be able to write
     */
    @Test(timeout=30000)
    public void testMultiMessageAuthFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                FailAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startBookie(bookieConf);
        try {
            connectAndWriteToBookie(clientConf); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
    }

    /**
     * Test that when the bookie and the client have a different
     * plugin configured, no messages will get through.
     */
    @Test(timeout=30000)
    public void testDifferentPluginFailure() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                DifferentPluginBookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName());
        
        startBookie(bookieConf);
        try {
            connectAndWriteToBookie(clientConf); // should fail
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
    }

    /**
     * Test that when the plugin class does exist, but
     * doesn't implement the interface, we fail predictably
     */
    @Test(timeout=30000)
    public void testExistantButNotValidPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "java.lang.String");

        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "java.lang.String");
        try {
            startBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + BookieAuthProvider.Factory.class.getName()));
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertTrue("Wrong exception thrown",
                    e.getMessage().contains("not "
                            + ClientAuthProvider.Factory.class.getName()));
        }
    }

    /**
     * Test that when the plugin class does not exist,
     * the bookie will not start and the client will
     * break.
     */
    @Test(timeout=30000)
    public void testNonExistantPlugin() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                "NonExistantClassNameForTestingAuthPlugins");
        try {
            startBookie(bookieConf);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }

        try {
            BookKeeper bkc = new BookKeeper(clientConf, zkc);
            fail("Shouldn't get this far");
        } catch (RuntimeException e) {
            // received correct exception
            assertEquals("Wrong exception thrown",
                    e.getCause().getClass(), ClassNotFoundException.class);
        }
    }

    /**
     * Test that when the plugin on the bookie crashes, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test(timeout=30000)
    public void testCrashDuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashAfter3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName())
            .setAuthTimeout(1000);
        startBookie(bookieConf);

        try {
            connectAndWriteToBookie(clientConf);
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
    }

    /**
     * Test that when a bookie simply stops replying during auth, the client doesn't
     * hang also, but it cannot write in any case.
     */
    @Test(timeout=30000)
    public void testCrashType2DuringAuth() throws Exception {
        ServerConfiguration bookieConf = newServerConfiguration();
        bookieConf.setBookieAuthProviderFactoryClass(
                CrashType2After3BookieAuthProviderFactory.class.getName());
        
        ClientConfiguration clientConf = newClientConfiguration();
        clientConf.setClientAuthProviderFactoryClass(
                SendUntilCompleteClientAuthProviderFactory.class.getName())
            .setAuthTimeout(1000);
        startBookie(bookieConf);

        try {
            connectAndWriteToBookie(clientConf);
            fail("Shouldn't get this far");
        } catch (BKException.BKUnauthorizedAccessException bke) {
            // bookie should have sent a negative response before
            // breaking the conneciton
        }
    }
      
    public static class AlwaysSucceedBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {

                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    builder.setExtension(TestDataFormats.messageType, 
                            TestDataFormats.AuthMessageType.SUCCESS_RESPONSE);

                    cb.operationComplete(BKException.Code.OK, builder.build());
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

    public static class AlwaysFailBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    builder.setExtension(TestDataFormats.messageType, 
                            TestDataFormats.AuthMessageType.FAILURE_RESPONSE);

                    cb.operationComplete(BKException.Code.OK, builder.build());
                    completeCb.operationComplete(
                            BKException.Code.UnauthorizedAccessException, null);
                }
            };
        }
    }

    private static class SendUntilCompleteClientAuthProviderFactory
        implements ClientAuthProvider.Factory {
        
        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ClientConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public ClientAuthProvider newProvider(InetSocketAddress addr,
                final GenericCallback<Void> completeCb) {
            AuthMessage.Builder builder
                = AuthMessage.newBuilder()
                .setAuthPluginName(getPluginName());
            builder.setExtension(TestDataFormats.messageType, 
                                 TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);
            final AuthMessage message = builder.build();

            return new ClientAuthProvider() {
                public void init(GenericCallback<AuthMessage> cb) {
                    cb.operationComplete(BKException.Code.OK, message);
                }

                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    if (m.hasExtension(TestDataFormats.messageType)) {
                        TestDataFormats.AuthMessageType type
                            = m.getExtension(TestDataFormats.messageType);
                        if (type == TestDataFormats.AuthMessageType.SUCCESS_RESPONSE) {
                            completeCb.operationComplete(BKException.Code.OK, null);
                        } else if (type == TestDataFormats.AuthMessageType.FAILURE_RESPONSE) {
                            completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                        } else {
                            cb.operationComplete(BKException.Code.OK, message);
                        }
                    } else {
                        completeCb.operationComplete(BKException.Code.UnauthorizedAccessException, null);
                    }
                }
            };
        }
    }

    public static class SucceedAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    if (numMessages.incrementAndGet() == 3) {
                        builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.SUCCESS_RESPONSE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else {
                        builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                    }
                }
            };
        }
    }

    public static class FailAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    if (numMessages.incrementAndGet() == 3) {
                        builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.FAILURE_RESPONSE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                        completeCb.operationComplete(BKException.Code.OK, null);
                    } else {
                        builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                    }
                }
            };
        }
    }

    public static class CrashAfter3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    if (numMessages.incrementAndGet() == 3) {
                        throw new RuntimeException("Do bad things to the bookie");
                    } else {
                        builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                    }
                }
            };
        }
    }

    public static class CrashType2After3BookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        AtomicInteger numMessages = new AtomicInteger(0);

        @Override
        public String getPluginName() {
            return TEST_AUTH_PROVIDER_PLUGIN_NAME;
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            completeCb.operationComplete(BKException.Code.OK, null);
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {
                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    if (numMessages.incrementAndGet() != 3) {
                         builder.setExtension(TestDataFormats.messageType, 
                                TestDataFormats.AuthMessageType.PAYLOAD_MESSAGE);

                        cb.operationComplete(BKException.Code.OK, builder.build());
                        return;
                     }

                    final CountDownLatch l = new CountDownLatch(1);
                    final String name = "NIOServer-";
                    Thread[] allthreads = new Thread[Thread.activeCount()];
                    Thread.enumerate(allthreads);
                    for (final Thread t : allthreads) {
                        if (t.getName().startsWith(name)) {
                            Thread sleeper = new Thread() {
                                    @Override
                                    public void run() {
                                        try {
                                            l.await();
                                            t.suspend();
                                        } catch (Exception e) {
                                            LOG.error("Error suspending thread", e);
                                        }
                                    }
                                };
                            sleeper.start();
                        }
                    }
                    l.countDown();
                }
            };
        }
    }

    public static class DifferentPluginBookieAuthProviderFactory
        implements BookieAuthProvider.Factory {
        @Override
        public String getPluginName() {
            return "DifferentAuthProviderPlugin";
        }

        @Override
        public void init(ServerConfiguration conf, ExtensionRegistry registry) {
            TestDataFormats.registerAllExtensions(registry);
        }

        @Override
        public BookieAuthProvider newProvider(InetSocketAddress addr,
                                              final GenericCallback<Void> completeCb) {
            return new BookieAuthProvider() {
                public void process(AuthMessage m, GenericCallback<AuthMessage> cb) {

                    AuthMessage.Builder builder
                        = AuthMessage.newBuilder()
                        .setAuthPluginName(getPluginName());
                    builder.setExtension(TestDataFormats.messageType, 
                            TestDataFormats.AuthMessageType.FAILURE_RESPONSE);

                    cb.operationComplete(BKException.Code.OK, builder.build());
                    completeCb.operationComplete(BKException.Code.OK, null);
                }
            };
        }
    }

}
