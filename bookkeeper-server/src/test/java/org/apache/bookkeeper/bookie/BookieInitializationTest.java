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

import java.io.File;
import java.net.BindException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.channel.ChannelException;
import junit.framework.Assert;

import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.StateMachine;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeperAdmin;

import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.fail;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    ZooKeeper newzk = null;
    
    public BookieInitializationTest() {
        super(0);
    }

    @Override
    public void tearDown() throws Exception {
        if (null != newzk) {
            newzk.close();
        }
        super.tearDown();
    }

    static class DummyFatalErrorHandler implements Registrar.FatalErrorHandler {
        CountDownLatch latch = new CountDownLatch(1);

        boolean awaitFatalError(int count, TimeUnit unit) throws InterruptedException {
            return latch.await(count, unit);
        }

        @Override
        public void fatalError(Throwable t) {
            latch.countDown();
            LOG.error("Fatal error", t);
        }
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test(timeout = 20000)
    public void testBookieRegistration() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(3000);
        final String testId = "testId:1234";
        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/" + testId;

        Registrar r = new Registrar(conf, testId, new DummyFatalErrorHandler());
        r.register().get();

        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        Registrar r2 = new Registrar(conf, testId, new DummyFatalErrorHandler());

        Future<Void> f = r2.register();
        Thread.sleep(conf.getZkTimeout() / 3);
        r.close();

        f.get();
        // verify ephemeral owner of the bkReg znode
        Stat bkRegNode2 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
        Assert.assertTrue("Bookie is referring to old registration znode:"
                          + bkRegNode1 + ", New ZNode:" + bkRegNode2,
                bkRegNode1.getEphemeralOwner() != bkRegNode2.getEphemeralOwner());

        r2.close();
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test(timeout = 30000)
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(3000);
        final String testId = "testId:1234";
        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/" + testId;

        Registrar r = new Registrar(conf, testId, new DummyFatalErrorHandler());
        r.register().get();

        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!", bkRegNode1);

        DummyFatalErrorHandler errorHandler = new DummyFatalErrorHandler();
        Registrar r2 = new Registrar(conf, testId, errorHandler);
        try {
            r2.register().get();
            fail("Shouldn't get to here");
        } catch (ExecutionException ee) {
            Assert.assertEquals("Should be a node exists exception",
                    KeeperException.NodeExistsException.class, ee.getCause().getClass());
        }
        // verify ephemeral owner of the bkReg znode
        Stat bkRegNode2 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
        Assert.assertTrue("Bookie wrongly registered. Old registration znode:"
                          + bkRegNode1 + ", New znode:" + bkRegNode2,
                          bkRegNode1.getEphemeralOwner() == bkRegNode2.getEphemeralOwner());
        Assert.assertTrue("Fatal error handler should have triggered",
                          errorHandler.awaitFatalError(5, TimeUnit.SECONDS));
        r.close();
        r2.close();
    }

    /**
     * Verify duplicate bookie server startup. Should throw
     * java.net.BindException if already BK server is running
     */
    @Test(timeout = 20000)
    public void testDuplicateBookieServerStartup() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(
                tmpDir.getPath()).setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        BookieServer bs1 = new BookieServer(conf);
        bs1.start();

        // starting bk server with same conf
        try {
            BookieServer bs2 = new BookieServer(conf);
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (ChannelException ce) {
            Assert.assertTrue("Should be caused by a bind exception",
                              ce.getCause() instanceof BindException);
            Assert.assertTrue("BKServer allowed duplicate startups!",
                    ce.getCause().getMessage().contains("Address already in use"));
        }
    }

    /**
     * Verify bookie start behaviour when ZK Server is not running.
     */
    @Test(timeout = 20000)
    public void testStartBookieWithoutZKServer() throws Exception {
        zkUtil.killServer();

        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        try {
            new Bookie(conf);
            fail("Should throw ConnectionLossException as ZKServer is not running!");
        } catch (KeeperException.ConnectionLossException e) {
            // expected behaviour
        } 
    }

    /**
     * Verify that if I try to start a bookie without zk initialized, it won't
     * prevent me from starting the bookie when zk is initialized
     */
    @Test(timeout = 20000)
    public void testStartBookieWithoutZKInitialized() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final String ZK_ROOT = "/ledgers2";

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setZkLedgersRootPath(ZK_ROOT);
        try {
            new Bookie(conf);
            fail("Should throw NoNodeException");
        } catch (Exception e) {
            // shouldn't be able to start
        }
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        clientConf.setZkLedgersRootPath(ZK_ROOT);
        BookKeeperAdmin.format(clientConf, false, false);

        Bookie b = new Bookie(conf);
        b.shutdown();
    }

    /**
     * Check disk full. Expected to throw NoWritableLedgerDirException
     * during bookie initialisation.
     */
    @Test(timeout = 30000)
    public void testWithDiskFull() throws Exception {
        File tmpDir = createTempDir("DiskCheck", "test");
        long usableSpace = tmpDir.getUsableSpace();
        long totalSpace = tmpDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        conf.setDiskUsageThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.05f);
        conf.setDiskUsageWarnThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.25f);
        try {
            new Bookie(conf);
        } catch (Exception e) {
            // expected
        }
    }

    /**
     * Check disk error for file. Expected to throw DiskErrorException.
     */
    @Test(timeout = 30000)
    public void testWithDiskError() throws Exception {
        File parent = createTempDir("DiskCheck", "test");
        File child = File.createTempFile("DiskCheck", "test", parent);
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(child.getPath())
                .setLedgerDirNames(new String[] { child.getPath() });
        try {
            // LedgerDirsManager#init() is used in Bookie instantiation.
            // Simulating disk errors by directly calling #init
            LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs());
            ldm.init();
        } catch (Exception e) {
            // expected
        }
    }

    public void waitForStateChange(StateMachine.Fsm fsm, StateMachine.State curState,
                                   int timeout, TimeUnit unit) throws Exception {
        long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);

        while (fsm.getCurrentState() == curState) {
            if (timeoutAt < System.nanoTime()) {
                throw new Exception("State didn't change in " + timeout + " " + unit);
            }
            Thread.sleep(100);
        }
    }

    /**
     * Verify that the registrar can reestablish a registration even
     * after a zookeeper session loss.
     */
    @Test(timeout=20000)
    public void testRegistrarLosingZKSessionAndReestablishing() throws Exception {
        final ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(1000);
        final String testId = "testId:1234";
        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/" + testId;

        Registrar r = new Registrar(conf, testId, new DummyFatalErrorHandler());
        r.register().get();

        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!", bkRegNode1);

        StateMachine.State curState = r.fsm.getCurrentState();
        Assert.assertEquals("Should be in registered state",
                curState.getClass(), Registrar.RegisteredState.class);
        zkUtil.expireSession(((Registrar.RegisteredState)curState).zk);

        // wait for expiration to be observed
        waitForStateChange(r.fsm, curState, 10, TimeUnit.SECONDS);

        r.register().get();
        r.close();
    }

    /**
     * Verify that if a registrar has transitioned to readonly start and it
     * loses its session, it will reconnect
     */
    @Test(timeout=20000)
    public void testRegistrarReadOnlyLosingZKSessionAndReestablishing() throws Exception {
        final ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(1000);
        final String testId = "testId:1234";

        Registrar r = new Registrar(conf, testId, new DummyFatalErrorHandler());
        r.register().get();
        r.registerReadOnly().get();

        Assert.assertNull("Reg node shouldn't exist",
                zkc.exists(r.getBookieRegistrationPath(), false));
        Assert.assertNotNull("RO Reg node doesn't exists!",
                zkc.exists(r.getBookieReadOnlyRegistrationPath(), false));

        StateMachine.State curState = r.fsm.getCurrentState();
        Assert.assertEquals("Should be in registered readonly state",
                curState.getClass(), Registrar.RegisteredReadOnlyState.class);
        zkUtil.expireSession(((Registrar.RegisteredReadOnlyState)curState).zk);

        // wait for expiration to be observed
        waitForStateChange(r.fsm, curState, 10, TimeUnit.SECONDS);

        r.registerReadOnly().get();
        r.close();
    }

    /*
     * Verify that if a registrar is transitioned to readonly while the zookeeper
     * session is lost, it will register correctly
     */
    @Test(timeout=20000)
    public void testRegistrarLoseZKSessionDuringTransition() throws Exception {
        final ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setZkTimeout(1000);
        final String testId = "testId:1234";

        Registrar r = new Registrar(conf, testId, new DummyFatalErrorHandler());
        r.register();

        zkUtil.stopServer();

        Thread.sleep(2*conf.getZkTimeout());

        zkUtil.restartServer();
        r.registerReadOnly().get();
        r.close();
    }

}
