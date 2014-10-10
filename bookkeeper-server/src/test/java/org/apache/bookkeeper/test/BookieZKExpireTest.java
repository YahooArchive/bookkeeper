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

package org.apache.bookkeeper.test;

import java.io.File;
import java.util.HashSet;
import java.util.List;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import com.google.common.collect.Lists;

public class BookieZKExpireTest extends BookKeeperClusterTestCase {

    public BookieZKExpireTest() {
        super(0);
        // 6000 is minimum due to default tick time
        baseConf.setZkTimeout(6000);
        baseClientConf.setZkTimeout(6000);
    }

    @Test(timeout = 60000)
    public void testBookieServerZKExpireBehaviour() throws Exception {
        BookieServer server = null;
        try {
            File f = File.createTempFile("bookieserver", "test");
            f.delete();
            f.mkdir();

            HashSet<Thread> threadset = new HashSet<Thread>();
            int threadCount = Thread.activeCount();
            Thread threads[] = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1) {
                    threadset.add(threads[i]);
                }
            }

            ServerConfiguration conf = newServerConfiguration(PortManager.nextFreePort(),
                    zkUtil.getZooKeeperConnectString(), f, new File[] { f });
            conf.setGcWaitTime(60000);
            server = new BookieServer(conf);
            server.start();

            int secondsToWait = 5;
            while (!server.isRunning()) {
                Thread.sleep(1000);
                if (secondsToWait-- <= 0) {
                    fail("Bookie never started");
                }
            }

            List<Thread> sendThreads = Lists.newArrayList();
            threadCount = Thread.activeCount();
            threads = new Thread[threadCount * 2];
            threadCount = Thread.enumerate(threads);
            for (int i = 0; i < threadCount; i++) {
                if (threads[i].getName().indexOf("SendThread") != -1 && !threadset.contains(threads[i])) {
                    sendThreads.add(threads[i]);
                    break;
                }
            }
            assertFalse("Send thread not found", sendThreads.isEmpty());

            for (Thread sendThread : sendThreads) {
                sendThread.suspend();
            }

            Thread.sleep(2 * conf.getZkTimeout());

            for (Thread sendThread : sendThreads) {
                sendThread.resume();
            }

            // allow watcher thread to run
            Thread.sleep(1000);

            assertTrue("Bookie should not have shutdown on losing zk session", server.isBookieRunning());
            assertTrue("Nio Server should not have shutdown on losing zk session", server.isNioServerRunning());
            assertTrue("Bookie Server should not have shutdown on losing zk session", server.isRunning());

            // Check that the bookie registration z-node has been re-created
            String myId = StringUtils.addrToString(Bookie.getBookieAddress(conf));
            String zkBookieRegPath = conf.getZkAvailableBookiesPath() + "/" + myId;
            ZooKeeper zkc = ZkUtils
                    .createConnectedZookeeperClient(conf.getZkServers(), new ZooKeeperWatcherBase(10000));
            assertTrue("Bookie did not register again after zk session expired",
                    zkc.exists(zkBookieRegPath, null) != null);
            zkc.close();
        } finally {
            server.shutdown();
        }
    }
}
