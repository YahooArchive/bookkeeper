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

import com.google.common.util.concurrent.AbstractFuture;

import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.Closeable;
import java.io.IOException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.StateMachine.DeferrableEvent;
import org.apache.bookkeeper.util.StateMachine.Event;
import org.apache.bookkeeper.util.StateMachine.Fsm;
import org.apache.bookkeeper.util.StateMachine.FsmImpl;
import org.apache.bookkeeper.util.StateMachine.State;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle bookie registration
 */
class Registrar implements Closeable {
    static Logger LOG = LoggerFactory.getLogger(Registrar.class);

    final ScheduledExecutorService executor;
    final Fsm fsm;
    private ServerConfiguration conf;
    private final String zkBookieRegPath;
    private final String zkBookieReadOnlyRegPath;
    private final FatalErrorHandler fatalErrorHandler;

    Registrar(ServerConfiguration conf, String myId, FatalErrorHandler handler) {
        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("bookie-reg"));
        this.conf = conf;

        this.zkBookieRegPath = conf.getZkAvailableBookiesPath() + "/" + myId;
        this.zkBookieReadOnlyRegPath = conf.getZkAvailableBookiesPath()
            + "/" + BookKeeperConstants.READONLY + "/" + myId;

        this.fatalErrorHandler = handler;
        fsm = new FsmImpl(executor);
        if (conf.getZkServers() != null) {
            fsm.setInitState(new UnregisteredState(fsm));
        } else {
            fsm.setInitState(new AnythingGoesState(fsm));
        }
    }

    String getBookieRegistrationPath() {
        return zkBookieRegPath;
    }

    String getBookieReadOnlyRegistrationPath() {
        return zkBookieReadOnlyRegPath;
    }

    Future<Void> register() {
        RegisterEvent e = new RegisterEvent();
        fsm.sendEvent(e);
        return e;
    }

    Future<Void> registerReadOnly() {
        RegisterReadOnlyEvent e = new RegisterReadOnlyEvent();
        fsm.sendEvent(e);
        return e;
    }

    @Override
    public void close() throws IOException {
        CloseEvent e = new CloseEvent();
        fsm.sendEvent(e);
        try {
            e.get();
        } catch (ExecutionException ee) {
            throw new IOException(ee.getCause());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOG.warn("Registrar executor didn't shut down cleanly");
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.warn("Registrar executor interrupted during shutdown");
        }
    }

    ZooKeeper createZooKeeper() throws KeeperException, InterruptedException, IOException {
        ZooKeeperWatcherBase watcher = new ZooKeeperWatcherBase(conf.getZkTimeout()) {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == KeeperState.Expired) {
                        fsm.sendEvent(new ZooKeeperError());
                    }
                    super.process(event);
                }
            };
        return ZkUtils.createConnectedZookeeperClient(conf.getZkServers(), watcher);
    }

    private void closeZooKeeper(ZooKeeper zk) {
        if (zk == null) {
            return;
        }
        try {
            zk.close();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted closing zookeeper handle");
        }
    }

    class CloseEvent extends AbstractFuture<Void> implements DeferrableEvent {
        void closed() {
            set(null);
        }

        @Override
        public void error(Throwable t) {
            setException(t);
        }
    }

    class RegisterEvent extends AbstractFuture<Void> implements DeferrableEvent {
        void registered() {
            set(null);
        }

        @Override
        public void error(Throwable t) {
            setException(t);
        }
    }
    class RegisterReadOnlyEvent extends RegisterEvent {}
    class ZooKeeperError implements Event {}

    class UnregisteredState extends State {
        UnregisteredState(Fsm fsm) {
            super(fsm);
        }

        public State handleEvent(RegisterEvent e) {
            fsm.deferEvent(e);
            return new RegisteringState(fsm);
        }
    }

    class RegisteringState extends State {
        private Future<?> retry = null;
        private ZooKeeper passedZookeeper = null;

        RegisteringState(Fsm fsm) {
            super(fsm);
        }

        RegisteringState(Fsm fsm, ZooKeeper zk) {
            this(fsm);
            passedZookeeper = zk;
        }
        /**
         * Handle the register event. Schedule a retry
         * if zookeeper is unavailable
         */
        public State handleEvent(RegisterEvent e) {
            ZooKeeper zk = null;
            try {
                if (passedZookeeper != null) {
                    zk = passedZookeeper;
                    passedZookeeper = null;
                } else {
                    zk = createZooKeeper();
                }
                PreviousNodeWatcher w = new PreviousNodeWatcher();
                if (null != zk.exists(zkBookieRegPath, w)) {
                    LOG.info("Previous bookie registration znode: " + zkBookieRegPath
                             + " exists, so waiting zk sessiontimeout: " + conf.getZkTimeout()
                             + "ms for znode deletion");
                    // waiting for the previous bookie reg znode deletion
                    if (!w.await(conf.getZkTimeout(), TimeUnit.MILLISECONDS)) {
                        fsm.deferEvent(e);
                        return new ErrorState(fsm, new NodeExistsException(zkBookieRegPath));
                    }
                }

                // Create the ZK ephemeral node for this Bookie.
                zk.create(zkBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                          CreateMode.EPHEMERAL);
                fsm.deferEvent(e);
                return new RegisteredState(fsm, zk);
            } catch (KeeperException.ConnectionLossException ke) {
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ke);
                retry = fsm.sendEvent(e, conf.getZkTimeout(), TimeUnit.MILLISECONDS);
                closeZooKeeper(zk);
                return this;
            } catch (KeeperException.SessionExpiredException ke) {
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ke);
                retry = fsm.sendEvent(e, conf.getZkTimeout(), TimeUnit.MILLISECONDS);
                closeZooKeeper(zk);
                return this;
            } catch (KeeperException ke) {
                closeZooKeeper(zk);
                return new ErrorState(fsm, ke);
            } catch (InterruptedException ie) {
                closeZooKeeper(zk);
                Thread.currentThread().interrupt();
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ie);
                return new ErrorState(fsm, ie);
            } catch (IOException ie) {
                closeZooKeeper(zk);
                return new ErrorState(fsm, ie);
            }
        }

        public State handleEvent(CloseEvent e) {
            if (retry != null) {
                retry.cancel(false);
            }
            e.closed();

            if (passedZookeeper != null) {
                closeZooKeeper(passedZookeeper);
            }
            return new ClosedState(fsm);
        }

        public State handleEvent(ZooKeeperError e) {
            // ignore, we only need to hear about this in registered state
            return this;
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            fsm.deferEvent(e);
            if (passedZookeeper != null) {
                closeZooKeeper(passedZookeeper);
            }
            return new RegisteringReadOnlyState(fsm);
        }
    }

    class RegisteredState extends State {
        final ZooKeeper zk;

        RegisteredState(Fsm fsm, ZooKeeper zk) {
            super(fsm);
            this.zk = zk;
        }

        public State handleEvent(RegisterEvent e) {
            e.registered();
            return this;
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            fsm.deferEvent(e);
            return new RegisteringReadOnlyState(fsm, zk);
        }

        public State handleEvent(CloseEvent e) {
            closeZooKeeper(zk);
            e.closed();
            return new ClosedState(fsm);
        }

        public State handleEvent(ZooKeeperError e) {
            closeZooKeeper(zk);

            // Trigger restarting of registration
            fsm.sendEvent(new RegisterEvent());
            return new RegisteringState(fsm);
        }
    }

    class RegisteringReadOnlyState extends State {
        final ZooKeeper zk;
        Future<?> retry = null;

        RegisteringReadOnlyState(Fsm fsm) {
            super(fsm);
            this.zk = null;
        }

        RegisteringReadOnlyState(Fsm fsm, ZooKeeper zk) {
            super(fsm);
            this.zk = zk;
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            try {
                ZooKeeper zk = this.zk == null ? createZooKeeper() : this.zk;

                PreviousNodeWatcher w = new PreviousNodeWatcher();
                if (null != zk.exists(zkBookieReadOnlyRegPath, w)) {
                    LOG.info("Previous bookie registration znode: " + zkBookieReadOnlyRegPath
                             + " exists, so waiting zk sessiontimeout: " + conf.getZkTimeout()
                             + "ms for znode deletion");
                    // waiting for the previous bookie reg znode deletion
                    if (!w.await((int)(conf.getZkTimeout() * 1.5), TimeUnit.MILLISECONDS)) {
                        fsm.deferEvent(e);
                        return new ErrorState(fsm, new NodeExistsException(zkBookieRegPath));
                    }
                }

                try {
                    // Clear the current registered node
                    zk.delete(zkBookieRegPath, -1);
                } catch (KeeperException.NoNodeException ke) {
                    // It may no be there, if not i dont care
                }

                // Create the ZK ephemeral node for this Bookie.
                ZkUtils.createFullPathOptimistic(zk, zkBookieReadOnlyRegPath,
                        new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                fsm.deferEvent(e);
                return new RegisteredReadOnlyState(fsm, zk);
            } catch (KeeperException.ConnectionLossException ke) {
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ke);
                retry = fsm.sendEvent(e, conf.getZkTimeout(), TimeUnit.MILLISECONDS);
                closeZooKeeper(zk);
                return this;
            } catch (KeeperException.SessionExpiredException ke) {
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ke);
                retry = fsm.sendEvent(e, conf.getZkTimeout(), TimeUnit.MILLISECONDS);
                return this;
            } catch (KeeperException ke) {
                return new ErrorState(fsm, ke);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("ZK exception registering ephemeral Znode for Bookie!", ie);
                return new ErrorState(fsm, ie);
            } catch (IOException ie) {
                return new ErrorState(fsm, ie);
            }
        }

        public State handleEvent(ZooKeeperError e) {
            // ignore, we only need to hear about this in registered state
            return this;
        }

        public State handleEvent(CloseEvent e) {
            if (retry != null) {
                retry.cancel(false);
            }
            e.closed();
            return new ClosedState(fsm);
        }
    }

    class RegisteredReadOnlyState extends RegisteredState {
        ZooKeeper zk;

        RegisteredReadOnlyState(Fsm fsm, ZooKeeper zk) {
            super(fsm, zk);
            this.zk = zk;
        }

        public State handleEvent(RegisterEvent e) {
            try {
                // Clear the current registered node
                zk.delete(zkBookieReadOnlyRegPath, -1);
            } catch (KeeperException.NoNodeException ke) {
                LOG.warn("Current readonly registration didn't exist, even though "
                         + "we're in registered readonly state", ke);
            } catch (KeeperException ke) {
                LOG.error("Error removing old read only registration", ke);
                closeZooKeeper(zk);
                fsm.deferEvent(e);
                return new RegisteringState(fsm);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.warn("Interrupted while cleaning up old readonly registration");
            }
            fsm.deferEvent(e);
            return new RegisteringState(fsm, zk);
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            e.registered();
            return this;
        }

        public State handleEvent(ZooKeeperError e) {
            closeZooKeeper(zk);

            // Trigger restarting of registration
            fsm.sendEvent(new RegisterReadOnlyEvent());
            return new RegisteringReadOnlyState(fsm);
        }

        // CloseEvent handle by parent class
    }

    class ErrorState extends State {
        Throwable t;

        ErrorState(Fsm fsm, Throwable t) {
            super(fsm);
            this.t = t;

            // this may trigger a shutdown of the bookie
            fatalErrorHandler.fatalError(t);
        }

        public State handleEvent(DeferrableEvent e) {
            e.error(t);
            return this;
        }

        public State handleEvent(CloseEvent e) {
            e.closed();
            return this;
        }
    }

    class ClosedState extends State {
        ClosedState(Fsm fsm) {
            super(fsm);
        }

        public State handleEvent(RegisterEvent e) {
            e.error(new Exception("Closed"));
            return this;
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            e.error(new Exception("Closed"));
            return this;
        }

        public State handleEvent(CloseEvent e) {
            e.closed();
            return this;
        }
    }

    class AnythingGoesState extends State {
        AnythingGoesState(Fsm fsm) {
            super(fsm);
        }

        public State handleEvent(RegisterEvent e) {
            e.registered();
            return this;
        }

        public State handleEvent(RegisterReadOnlyEvent e) {
            e.registered();
            return this;
        }

        public State handleEvent(CloseEvent e) {
            e.closed();
            return this;
        }
    }

    private static class PreviousNodeWatcher implements Watcher {
        final CountDownLatch prevNodeLatch = new CountDownLatch(1);

        boolean await(int timeout, TimeUnit unit) throws InterruptedException {
            return prevNodeLatch.await(timeout, unit);
        }

        @Override
        public void process(WatchedEvent event) {
            // Check for prev znode deletion. Connection expiration is
            // not handling, since bookie has logic to shutdown.
            if (EventType.NodeDeleted == event.getType()) {
                prevNodeLatch.countDown();
            }
        }
    }

    interface FatalErrorHandler {
        void fatalError(Throwable t);
    }
}

