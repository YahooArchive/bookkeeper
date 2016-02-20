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
package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.bookkeeper.auth.ClientAuthProvider;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperClientStats;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.TrimRequest;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashMap;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.ExtensionRegistry;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */
@Sharable
public class PerChannelBookieClient extends ChannelInboundHandlerAdapter {

    static final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);

    // this set contains the bookie error return codes that we do not consider for a bookie to be "faulty"
    private static final Set<Integer> expectedBkOperationErrors = Collections.unmodifiableSet(Sets
            .newHashSet(BKException.Code.BookieHandleNotAvailableException));

    public static final int MAX_FRAME_LENGTH = 5 * 1024 * 1024; // increased max netty frame size to 5Mb
    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    final BookieSocketAddress addr;
    final EventLoopGroup eventLoopGroup;
    final OrderedSafeExecutor executor;
    final long addEntryTimeoutNano;
    final long readEntryTimeoutNano;

    private final ConcurrentOpenHashMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentOpenHashMap<CompletionKey, CompletionValue>();

    // Map that hold duplicated read requests. The idea is to only use this map (synchronized) when there is a duplicate
    // read request for the same ledgerId/entryId
    private final ListMultimap<CompletionKey, CompletionValue> completionObjectsV2Conflicts = LinkedListMultimap
            .create();

    private final StatsLogger statsLogger;
    private final OpStatsLogger readEntryOpLogger;
    private final OpStatsLogger readTimeoutOpLogger;
    private final OpStatsLogger addEntryOpLogger;
    private final OpStatsLogger addTimeoutOpLogger;
    
    private final boolean useV2WireProtocol;

    /**
     * The following member variables do not need to be concurrent, or volatile
     * because they are always updated under a lock
     */
    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;

    enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;

    private final ClientAuthProvider.Factory authProviderFactory;
    private final ExtensionRegistry extRegistry;

    private final PerChannelBookieClientPool pcbcPool;

    public PerChannelBookieClient(OrderedSafeExecutor executor, EventLoopGroup eventLoopGroup,
                                  BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry) {
        this(new ClientConfiguration(), executor, eventLoopGroup, addr, authProviderFactory, extRegistry,
                NullStatsLogger.INSTANCE, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
            EventLoopGroup eventLoopGroup, BookieSocketAddress addr,
                                  ClientAuthProvider.Factory authProviderFactory,
                                  ExtensionRegistry extRegistry,
                                  StatsLogger parentStatsLogger,
                                  PerChannelBookieClientPool pcbcPool) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.eventLoopGroup = eventLoopGroup;
        this.state = ConnectionState.DISCONNECTED;
        this.useV2WireProtocol = conf.getUseV2WireProtocol();

        this.authProviderFactory = authProviderFactory;
        this.extRegistry = extRegistry;

        StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(addr.getHostname().replace('.', '_').replace('-', '_'))
            .append("_").append(addr.getPort());

        this.statsLogger = parentStatsLogger.scope(BookKeeperClientStats.CHANNEL_SCOPE)
            .scope(nameBuilder.toString());

        readEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_READ_OP);
        addEntryOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_ADD_OP);
        readTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_READ);
        addTimeoutOpLogger = statsLogger.getOpStatsLogger(BookKeeperClientStats.CHANNEL_TIMEOUT_ADD);
        addEntryTimeoutNano = TimeUnit.SECONDS.toNanos(conf.getAddEntryTimeout());
        readEntryTimeoutNano = TimeUnit.SECONDS.toNanos(conf.getReadEntryTimeout());

        this.pcbcPool = pcbcPool;
    }

    private void completeOperation(GenericCallback<PerChannelBookieClient> op, int rc) {
        closeLock.readLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                op.operationComplete(BKException.Code.ClientClosedException, this);
            } else {
                op.operationComplete(rc, this);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected ChannelFuture connect() {
        LOG.debug("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the bookie.
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        if (SystemUtils.IS_OS_LINUX && eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.option(ChannelOption.TCP_NODELAY, conf.getClientTcpNoDelay());
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, conf.getClientConnectTimeoutMillis());
        bootstrap.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, conf.getClientWriteBufferLowWaterMark());
        bootstrap.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, conf.getClientWriteBufferHighWaterMark());

        // If buffer sizes are 0, let OS to auto-tune it
        if (conf.getClientSendBufferSize() > 0) {
            bootstrap.option(ChannelOption.SO_SNDBUF, conf.getClientSendBufferSize());
        }

        if (conf.getClientReceiveBufferSize() > 0) {
            bootstrap.option(ChannelOption.SO_RCVBUF, conf.getClientReceiveBufferSize());
        }

        // In the netty pipeline, we need to split packets based on length, so we
        // use the {@link LengthFieldBasedFrameDecoder}. Other than that all actions
        // are carried out in this class, e.g., making sense of received messages,
        // prepending the length to outgoing packets etc.
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("lengthbasedframedecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.RequestEncoder(extRegistry));
                pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.ResponseDecoder(extRegistry));
                pipeline.addLast("authHandler", new AuthHandler.ClientSideHandler(authProviderFactory, txnIdGenerator));
                pipeline.addLast("mainhandler", PerChannelBookieClient.this);
            }
        });

        ChannelFuture future = bootstrap.connect(addr.getSocketAddress());
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                LOG.debug("Channel connected ({}) {}", future.isSuccess(), future.channel());
                int rc;
                Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {
                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        LOG.info("Successfully connected to bookie: {}", future.channel());
                        rc = BKException.Code.OK;
                        channel = future.channel();
                        state = ConnectionState.CONNECTED;
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.warn("Closed before connection completed, clean up: {}, current state {}",
                                 future.channel(), state);
                        closeChannel(future.channel());
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                        LOG.debug("Already connected with another channel({}), so close the new channel({})",
                                  channel, future.channel());
                        closeChannel(future.channel());
                        return; // pendingOps should have been completed when other channel connected
                    } else {
                        LOG.error("Could not connect to bookie: {}/{}, current state {} : ",
                                  new Object[] { future.channel(), addr,
                                                 state, future.cause() });
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        closeChannel(future.channel());
                        channel = null;
                        if (state != ConnectionState.CLOSED) {
                            state = ConnectionState.DISCONNECTED;
                        }
                    }

                    // trick to not do operations under the lock, take the list
                    // of pending ops and assign it to a new variable, while
                    // emptying the pending ops by just assigning it to a new
                    // list
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
                }

                for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                    completeOperation(pendingOp, rc);
                }
            }
        });

        return future;
    }

    void connectIfNeededAndDoOp(GenericCallback<PerChannelBookieClient> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            completeOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING) {
                        // just return as connection request has already send
                        // and waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            completeOperation(op, opRc);
        }

    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     *          Ledger Id
     * @param masterKey
     *          Master Key
     * @param entryId
     *          Entry Id
     * @param toSend
     *          Buffer to send
     * @param cb
     *          Write callback
     * @param ctx
     *          Write callback context
     * @param options
     *          Add options
     */
    void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ByteBuf toSend, WriteCallback cb,
                  Object ctx, final int options) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            request = BookieProtocol.AddRequest.create(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    (short) options, masterKey, toSend);
            completion = V2CompletionKey.get(this, ledgerId, entryId, OperationType.ADD_ENTRY);
            completionObjects.put(completion, AddCompletion.get(this, cb, ctx, ledgerId, entryId, completion));
        } else {
            final long txnId = getTxnId();
            final CompletionKey completionKey = new CompletionKey(this, txnId, OperationType.ADD_ENTRY);
            completionObjects.put(completionKey, AddCompletion.get(this, cb, ctx, ledgerId, entryId, completionKey));

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.ADD_ENTRY)
                    .setTxnId(txnId);

            byte[] toSendArray = new byte[toSend.readableBytes()];
            toSend.getBytes(toSend.readerIndex(), toSendArray);
            AddRequest.Builder addBuilder = AddRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(ByteString.copyFrom(masterKey))
                    .setBody(ByteString.copyFrom(toSendArray));

            if (((short)options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
            }

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setAddRequest(addBuilder)
                    .build();
        }

        final Object addRequest = request;
        final CompletionKey completionKey = completion;

        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            toSend.release();
            return;
        }
        try {
            // If the write fails, the connection will break and the requests will be marked as failed
            c.writeAndFlush(addRequest, c.voidPromise());
        } catch (Throwable e) {
            LOG.warn("Add entry operation failed", e);
            errorOutAddKey(completionKey);
        }
    }

    /**
     * This method should be called only after connection has been checked for
     * {@link #connectIfNeededAndDoOp(GenericCallback)}
     *
     * @param ledgerId
     * @param masterKey
     * @param lastEntryId
     * @param cb
     * @param ctx
     * @param options
     */
    void trim(final long ledgerId, byte[] masterKey, final long lastEntryId, final int options) {
        Object request = null;
        
        if (useV2WireProtocol) {
            request = new BookieProtocol.TrimRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, lastEntryId);
        } else {
            final long txnId = getTxnId();
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.TRIM_LEDGER)
                    .setTxnId(txnId);
            TrimRequest.Builder trimBuilder = TrimRequest.newBuilder()
                    .setLedgerId(ledgerId).setEntryId(lastEntryId);

            request = Request.newBuilder()
                    .setHeader(headerBuilder.build())
                    .setTrimRequest(trimBuilder.build())
                    .build();
        }

        final Channel c = channel;
        if (c == null) {
            LOG.warn("No channel exists for sending trim message {}@{} {}",
                     new Object[] { lastEntryId, ledgerId, addr });
            return;
        }

        try {
            c.writeAndFlush(request, c.voidPromise());
        } catch(Throwable e) {
            LOG.warn("Trim operation (lid={}, eid={}) to channel {} failed",
                     new Object[] { ledgerId, lastEntryId, c, e });
        }
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey,
                                        final long entryId,
                                        ReadEntryCallback cb, Object ctx) {
        CompletionKey completion = null;

        Object request = null;
        if (useV2WireProtocol) {
            completion = V2CompletionKey.get(this, ledgerId, entryId, OperationType.READ_ENTRY);
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    BookieProtocol.FLAG_DO_FENCING, masterKey);
        } else {
            final long txnId = getTxnId();
            completion = new CompletionKey(this, txnId, OperationType.READ_ENTRY);

            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);

            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setMasterKey(ByteString.copyFrom(masterKey))
                    .setFlag(ReadRequest.Flag.FENCE_LEDGER);

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }

        final CompletionKey completionKey = completion;
        ReadCompletion readCompletion = new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId);
        CompletionValue existingValue = completionObjects.putIfAbsent(completion, readCompletion);
        if (existingValue != null) {
            // There's a pending read request on same ledger/entry. Use the multimap to track all of them
            synchronized (this) {
                completionObjectsV2Conflicts.put(completionKey, readCompletion);
            }
        }

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }
        
        final Object readRequest = request;

        try {
            ChannelFuture future = c.writeAndFlush(readRequest);
            future.addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        if (future.isSuccess()) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Successfully wrote request {} to {}",
                                          readRequest, c.remoteAddress());
                            }
                        } else {
                            if (!(future.cause() instanceof ClosedChannelException)) {
                                LOG.warn("Writing readEntryAndFenceLedger(lid={}, eid={}) to channel {} failed : ",
                                        new Object[] { ledgerId, entryId, c, future.cause() });
                            }
                            errorOutReadKey(completionKey);
                        }
                    }
                });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", completionKey, e);
            errorOutReadKey(completionKey);
        }
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        Object request = null;
        CompletionKey completion = null;
        if (useV2WireProtocol) {
            request = new BookieProtocol.ReadRequest(BookieProtocol.CURRENT_PROTOCOL_VERSION, ledgerId, entryId,
                    (short) 0);
            completion = V2CompletionKey.get(this, ledgerId, entryId, OperationType.READ_ENTRY);
        } else {
            final long txnId = getTxnId();
            completion = new CompletionKey(this, txnId, OperationType.READ_ENTRY);
            // Build the request and calculate the total size to be included in the packet.
            BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                    .setVersion(ProtocolVersion.VERSION_THREE)
                    .setOperation(OperationType.READ_ENTRY)
                    .setTxnId(txnId);

            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId);

            request = Request.newBuilder()
                    .setHeader(headerBuilder)
                    .setReadRequest(readBuilder)
                    .build();
        }

        final Object readRequest = request;
        final CompletionKey completionKey = completion;
        ReadCompletion readCompletion = new ReadCompletion(this, readEntryOpLogger, cb, ctx, ledgerId, entryId);
        CompletionValue existingValue = completionObjects.putIfAbsent(completion, readCompletion);
        if (existingValue != null) {
            // There's a pending read request on same ledger/entry. Use the multimap to track all of them
            synchronized (this) {
                completionObjectsV2Conflicts.put(completionKey, readCompletion);
            }
        }

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        try{
            ChannelFuture future = c.writeAndFlush(readRequest);
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.isSuccess()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Successfully wrote request {} to {}",
                                      readRequest, c.remoteAddress());
                        }
                    } else {
                        if (!(future.cause() instanceof ClosedChannelException)) {
                            LOG.warn("Writing readEntry(lid={}, eid={}) to channel {} failed : ",
                                    new Object[] { ledgerId, entryId, c, future.cause() });
                        }
                        errorOutReadKey(completionKey);
                    }
                }
            });
        } catch(Throwable e) {
            LOG.warn("Read entry operation {} failed", readRequest, e);
            errorOutReadKey(completionKey);
        }
    }

    public void monitorPendingOperations() {
        int timedoutOperations = 0;
        for (CompletionKey key : completionObjects.keys()) {
            CompletionValue value = completionObjects.get(key);
            if (checkAndFailOperation(key, value)) {
                ++timedoutOperations;
            }
        }
        synchronized (this) {
            for (CompletionKey key : completionObjectsV2Conflicts.keys()) {
                for (CompletionValue value : completionObjectsV2Conflicts.get(key)) {
                    if (checkAndFailOperation(key, value)) {
                        ++timedoutOperations;
                    }
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Successfully timed-out {} operations to channel {} for {}", new Object[] { timedoutOperations,
                    channel, addr });
        }
    }

    private boolean checkAndFailOperation(CompletionKey key, CompletionValue value) {
        if (value != null) {
            long elapsedTime = MathUtils.elapsedNanos(value.startTime);
            if (OperationType.ADD_ENTRY == key.operationType) {
                if (elapsedTime >= addEntryTimeoutNano) {
                    errorOutAddKey(key, BKException.Code.TimeoutException);
                    addTimeoutOpLogger.registerSuccessfulEvent(elapsedTime, TimeUnit.NANOSECONDS);
                    return true;
                }
            } else {
                if (elapsedTime >= readEntryTimeoutNano) {
                    errorOutReadKey(key, BKException.Code.TimeoutException);
                    readTimeoutOpLogger.registerSuccessfulEvent(elapsedTime, TimeUnit.NANOSECONDS);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        disconnect(true);
    }

    public void disconnect(boolean wait) {
        LOG.info("Disconnecting the per channel bookie client for {}", addr);
        closeInternal(false, wait);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        close(true);
    }

    public void close(boolean wait) {
        LOG.info("Closing the per channel bookie client for {}", addr);
        closeLock.writeLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                return;
            }
            state = ConnectionState.CLOSED;
            errorOutOutstandingEntries(BKException.Code.ClientClosedException);
        } finally {
            closeLock.writeLock().unlock();
        }
        closeInternal(true, wait);
    }

    private void closeInternal(boolean permanent, boolean wait) {
        Channel toClose = null;
        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
            toClose = channel;
            channel = null;
        }
        if (toClose != null) {
            ChannelFuture cf = closeChannel(toClose);
            if (wait) {
                cf.awaitUninterruptibly();
            }
        }

    }

    private ChannelFuture closeChannel(Channel c) {
        LOG.debug("Closing channel {}", c);
        return c.close();
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        LOG.debug("Removing completion key: {}", key);
        ReadCompletion completion = (ReadCompletion) completionObjects.remove(key);
        if (completion == null) {
            // If there's no completion object here, try in the multimap
            synchronized (this) {
                if (completionObjectsV2Conflicts.containsKey(key)) {
                    completion = (ReadCompletion) completionObjectsV2Conflicts.get(key).get(0);
                    completionObjectsV2Conflicts.remove(key, completion);
                }
            }
        }

        // If it's still null, give up
        if (null == completion) {
            return;
        }

        final ReadCompletion readCompletion = completion;
        executor.submitOrdered(readCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if (c != null) {
                    bAddress = c.remoteAddress().toString();
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for reading entry: {} ledger-id: {} bookie: {} rc: {}",
                            new Object[] { readCompletion.entryId, readCompletion.ledgerId, bAddress, rc });
                }

                readCompletion.cb.readEntryComplete(rc, readCompletion.ledgerId, readCompletion.entryId,
                                                    null, readCompletion.ctx);
            }
        });
    }

    void errorOutAddKey(final CompletionKey key) {
        errorOutAddKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutAddKey(final CompletionKey key, final int rc) {
        AddCompletion completion = (AddCompletion) completionObjects.remove(key);

        if (null == completion) {
            return;
        }
        final AddCompletion addCompletion = completion;
        executor.submitOrdered(addCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                Channel c = channel;
                if(c != null) {
                    bAddress = c.remoteAddress().toString();
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {} rc: {}",
                            new Object[] { addCompletion.entryId, addCompletion.ledgerId, bAddress, rc });
                }

                addCompletion.cb.writeComplete(rc, addCompletion.ledgerId, addCompletion.entryId,
                                               addr, addCompletion.ctx);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Invoked callback method: {}", addCompletion.entryId);
                }

                addCompletion.recycle();
            }
        });
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. The one who
        // successfully removes the key from the map is the one responsible for
        // calling the application callback.
        for (CompletionKey key : completionObjects.keys()) {
            switch (key.operationType) {
            case ADD_ENTRY:
                errorOutAddKey(key, rc);
                break;
            case READ_ENTRY:
                errorOutReadKey(key, rc);
                break;
            default:
                break;
            }
        }
    }

    void recordError() {
        if (pcbcPool != null) {
            pcbcPool.recordError();
        }
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        LOG.info("Disconnected from bookie channel {}", ctx.channel());
        if (ctx.channel() != null) {
            closeChannel(ctx.channel());
        }

        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);

        synchronized (this) {
            if (this.channel == ctx.channel()
                && state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof CorruptedFrameException || cause instanceof TooLongFrameException) {
            LOG.error("Corrupted frame received from bookie: {}", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }

        if (cause instanceof AuthHandler.AuthenticationException) {
            LOG.error("Error authenticating connection", cause);
            errorOutOutstandingEntries(BKException.Code.UnauthorizedAccessException);
            Channel c = ctx.channel();
            if (c != null) {
                closeChannel(c);
            }
            return;
        }

        if (cause instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            ctx.close();
            return;
        }

        synchronized (this) {
            if (state == ConnectionState.CLOSED) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unexpected exception caught by bookie client channel handler, "
                            + "but the client is closed, so it isn't important", cause);
                }
            } else {
                LOG.error("Unexpected exception caught by bookie client channel handler", cause);
            }
        }

        // Since we are a library, cant terminate App here, can we?
        ctx.close();
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof BookieProtocol.Response) {
            BookieProtocol.Response response = (BookieProtocol.Response) msg;
            readV2Response(response);
        } else if (msg instanceof Response) {
            Response response = (Response) msg;
            readV3Response(response);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private void readV2Response(final BookieProtocol.Response response) {
        OperationType operationType = getOperationType(response.getOpCode());
        StatusCode status = getStatusCodeFromErrorCode(response.errorCode);
        
        V2CompletionKey key = V2CompletionKey.get(this, response.ledgerId, response.entryId, operationType);
        CompletionValue completionValue = completionObjects.remove(key);
        if (completionValue == null) {
            // If there's no completion object here, try in the multimap
            synchronized (this) {
                if (completionObjectsV2Conflicts.containsKey(key)) {
                    completionValue = completionObjectsV2Conflicts.get(key).get(0);
                    completionObjectsV2Conflicts.remove(key, completionValue);
                }
            }
        }

        key.recycle();

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + operationType
                        + " and ledger:entry : " + response.ledgerId + ":" + response.entryId);
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            
            executor.submitOrdered(orderingKey,
                    ReadV2ResponseCallback.create(this, status, operationType, response.ledgerId, response.entryId, completionValue, response));
        }
    }

    private static class ReadV2ResponseCallback extends SafeRunnable {
        PerChannelBookieClient pcbc;
        OperationType operationType;
        StatusCode status;
        long ledgerId;
        long entryId;
        CompletionValue completionValue;
        BookieProtocol.Response response;

        static ReadV2ResponseCallback create(PerChannelBookieClient pcbc, StatusCode status,
                OperationType operationType, long ledgerId, long entryId, CompletionValue completionValue,
                BookieProtocol.Response response) {
            ReadV2ResponseCallback callback = RECYCLER.get();
            callback.pcbc = pcbc;
            callback.status = status;
            callback.operationType = operationType;
            callback.ledgerId = ledgerId;
            callback.entryId = entryId;
            callback.completionValue = completionValue;
            callback.response = response;
            return callback;
        }

        @Override
        public void safeRun() {
            switch (operationType) {
            case ADD_ENTRY: {
                pcbc.handleAddResponse(status, ledgerId, entryId, completionValue);
                response.recycle();
                break;
            }
            case READ_ENTRY: {
                BookieProtocol.ReadResponse readResponse = (BookieProtocol.ReadResponse) response;
                pcbc.handleReadResponse(status, readResponse.getLedgerId(), readResponse.getEntryId(),
                        readResponse.data, completionValue);
                readResponse.recycle();
                break;
            }
            default:
                LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring", operationType, pcbc.addr);
                break;
            }

            recycle();
        }

        void recycle() {
            pcbc = null;
            status = null;
            operationType = null;
            ledgerId = -1;
            entryId = -1;
            completionValue = null;
            response = null;
            RECYCLER.recycle(this, recyclerHandle);
        }

        private final Handle recyclerHandle;

        private ReadV2ResponseCallback(Handle recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        private static final Recycler<ReadV2ResponseCallback> RECYCLER = new Recycler<ReadV2ResponseCallback>() {
            @Override
            protected ReadV2ResponseCallback newObject(Handle handle) {
                return new ReadV2ResponseCallback(handle);
            }
        };
    }
    
    private static OperationType getOperationType(byte opCode) {
        switch (opCode) {
        case BookieProtocol.ADDENTRY:
            return OperationType.ADD_ENTRY;
        case BookieProtocol.READENTRY:
            return OperationType.READ_ENTRY;
        case BookieProtocol.AUTH:
            return OperationType.AUTH;
        case BookieProtocol.TRIM:
            return OperationType.AUTH;
        default:
            throw new IllegalArgumentException("Invalid operation type");
        }
    }

    private void readV3Response(final Response response) {
        final BKPacketHeader header = response.getHeader();

        final CompletionValue completionValue = completionObjects.remove(newCompletionKey(header.getTxnId(),
                    header.getOperation()));

        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + header.getOperation()
                        + " and txnId : " + header.getTxnId());
            }
        } else {
            long orderingKey = completionValue.ledgerId;
            
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    OperationType type = header.getOperation();
                    switch (type) {
                        case ADD_ENTRY: {
                            AddResponse addResponse = response.getAddResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? addResponse.getStatus() : response.getStatus();
                            handleAddResponse(status, addResponse.getLedgerId(), addResponse.getEntryId(), completionValue);
                            break;
                        }
                        case READ_ENTRY: {
                            ReadResponse readResponse = response.getReadResponse();
                            StatusCode status = response.getStatus() == StatusCode.EOK ? readResponse.getStatus() : response.getStatus();
                            ByteBuf body = Unpooled.EMPTY_BUFFER;
                            if (readResponse.hasBody()) {
                                body = Unpooled.wrappedBuffer(readResponse.getBody().asReadOnlyByteBuffer());
                            }
                            handleReadResponse(status, readResponse.getLedgerId(), readResponse.getEntryId(), body, completionValue);
                            break;
                        }
                        default:
                            LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring",
                                      type, addr);
                            break;
                    }
                }
            });
        }
    }

    void handleAddResponse(StatusCode status, long ledgerId, long entryId, CompletionValue completionValue) {
        // The completion value should always be an instance of an AddCompletion object when we reach here.
        AddCompletion ac = (AddCompletion)completionValue;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + status);
        }
        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add for ledger: " + ledgerId + ", entry: " + entryId + " failed on bookie: " + addr
                        + " with code:" + status);
            }
            rcToRet = BKException.Code.WriteException;
        }
        ac.cb.writeComplete(rcToRet, ledgerId, entryId, addr, ac.ctx);
        ac.recycle();
    }

    void handleReadResponse(StatusCode status, long ledgerId, long entryId, ByteBuf body, CompletionValue completionValue) {
        // The completion value should always be an instance of a ReadCompletion object when we reach here.
        ReadCompletion rc = (ReadCompletion)completionValue;

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read entry for ledger:{}, entry:{} failed on bookie:{} with code:{}",
                      new Object[] { ledgerId, entryId, addr, status });
            rcToRet = BKException.Code.ReadException;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                    + entryId + " rc: " + rcToRet + " entry length: " + body.readableBytes());
        }

        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, body.slice(), rc.ctx);
    }

    /**
     * Boiler-plate wrapper classes follow
     *
     */

    // visible for testing
    static abstract class CompletionValue {
        final Object ctx;
        protected long ledgerId;
        protected long entryId;
        protected long startTime;

        public CompletionValue() {
            this.ctx = null;
            this.ledgerId = -1;
            this.entryId = -1;
            this.startTime = MathUtils.nowInNano();
        }

        public CompletionValue(Object ctx, long ledgerId, long entryId) {
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.startTime = MathUtils.nowInNano();
        }
    }

    // visible for testing
    static class ReadCompletion extends CompletionValue implements ReadEntryCallback {
        final ReadEntryCallback cb;
        final OpStatsLogger readEntryOpLogger;
        final ReadEntryCallback originalCallback;
        final Object ctx;
        final PerChannelBookieClient pcbc;

        public ReadCompletion(final PerChannelBookieClient pcbc, ReadEntryCallback cb, Object ctx,
                              long ledgerId, long entryId) {
            this(pcbc, null, cb, ctx, ledgerId, entryId);
        }

        public ReadCompletion(final PerChannelBookieClient pcbc, final OpStatsLogger readEntryOpLogger,
                              final ReadEntryCallback originalCallback,
                final Object originalCtx, final long ledgerId, final long entryId) {
            super(originalCtx, ledgerId, entryId);
            this.pcbc = pcbc;
            this.readEntryOpLogger = readEntryOpLogger;
            this.originalCallback = originalCallback;
            this.cb = this;
            this.ctx = originalCtx;
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId, ByteBuf buffer, Object ctx) {
            long latency = MathUtils.elapsedNanos(startTime);
            if (readEntryOpLogger != null) {
                if (rc != BKException.Code.OK) {
                    readEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                } else {
                    readEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                }
            }
            if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                pcbc.recordError();
            }

            originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, ctx);
        }
    }

    static class LedgerEntryPair {
        long ledgerId;
        long entryId;
    }

    // visible for testing
    static class AddCompletion extends CompletionValue implements WriteCallback {
        WriteCallback cb;
        PerChannelBookieClient pcbc;
        WriteCallback originalCallback;
        Object originalCtx;
        CompletionKey completionKey;

        public static AddCompletion get(PerChannelBookieClient pcbc, WriteCallback originalCallback, Object originalCtx,
 long ledgerId, long entryId, CompletionKey completionKey) {
            AddCompletion addCompletion = RECYCLER.get();
            addCompletion.originalCtx = originalCtx;
            addCompletion.ledgerId = ledgerId;
            addCompletion.entryId = entryId;
            addCompletion.pcbc = pcbc;
            addCompletion.startTime = MathUtils.nowInNano();
            addCompletion.originalCallback = originalCallback;
            addCompletion.cb = addCompletion;
            addCompletion.completionKey = completionKey;
            return addCompletion;
        }

        @Override
        public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
            if (pcbc.addEntryOpLogger != null) {
                long latency = MathUtils.elapsedNanos(startTime);
                if (rc != BKException.Code.OK) {
                    pcbc.addEntryOpLogger.registerFailedEvent(latency, TimeUnit.NANOSECONDS);
                } else {
                    pcbc.addEntryOpLogger.registerSuccessfulEvent(latency, TimeUnit.NANOSECONDS);
                }
            }
            if (rc != BKException.Code.OK && !expectedBkOperationErrors.contains(rc)) {
                pcbc.recordError();
            }

            originalCallback.writeComplete(rc, ledgerId, entryId, addr, originalCtx);
        }

        private final Handle recyclerHandle;

        private AddCompletion(Handle handle) {
            this.recyclerHandle = handle;
        }

        private static final Recycler<AddCompletion> RECYCLER = new Recycler<AddCompletion>() {
            protected AddCompletion newObject(Recycler.Handle handle) {
                return new AddCompletion(handle);
            }
        };

        public void recycle() {
            cb = null;
            pcbc = null;
            startTime = 0;
            originalCallback = null;
            originalCtx = null;

            if (completionKey != null) {
                completionKey.recycle();
                completionKey = null;
            }
            RECYCLER.recycle(this, recyclerHandle);
        }
    }

    // visable for testing
    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new CompletionKey(this, txnId, operationType);
    }

    static class CompletionKey {
        PerChannelBookieClient pcbc;
        long txnId;
        OperationType operationType;
        long requestAt;

        CompletionKey(PerChannelBookieClient pcbc, long txnId, OperationType operationType) {
            this.pcbc = pcbc;
            this.txnId = txnId;
            this.operationType = operationType;
            this.requestAt = MathUtils.nowInNano();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.txnId == that.txnId && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return ((int) txnId);
        }

        @Override
        public String toString() {
            return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
        }

        void recycle() {
        }
    }

    static class V2CompletionKey extends CompletionKey {
        long ledgerId;
        long entryId;

        static V2CompletionKey get(PerChannelBookieClient pcbc, long ledgerId, long entryId, OperationType operationType) {
            V2CompletionKey key = RECYCLER.get();
            key.pcbc = pcbc;
            key.operationType = operationType;
            key.txnId = -1;
            key.requestAt = MathUtils.nowInNano();
            key.ledgerId = ledgerId;
            key.entryId = entryId;
            return key;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof V2CompletionKey)) {
                return false;
            }
            V2CompletionKey that = (V2CompletionKey) obj;
            return this.ledgerId == that.ledgerId && this.entryId == that.entryId
                    && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(ledgerId) * 31 + Long.hashCode(entryId);
        }

        @Override
        public String toString() {
            return String.format("%d:%d %s", ledgerId, entryId, operationType);
        }

        private final Handle recyclerHandle;

        private V2CompletionKey(Handle handle) {
            super(null, -1, null);
            this.recyclerHandle = handle;
        }

        private static final Recycler<V2CompletionKey> RECYCLER = new Recycler<V2CompletionKey>() {
            protected V2CompletionKey newObject(Recycler.Handle handle) {
                return new V2CompletionKey(handle);
            }
        };

        @Override
        public void recycle() {
            pcbc = null;
            txnId = -1;
            requestAt = -1;
            ledgerId = -1;
            entryId = -1;
            RECYCLER.recycle(this, recyclerHandle);
        }
    }

    /**
     * Note : Helper functions follow
     */

    /**
     * @param status
     * @return null if the statuscode is unknown.
     */
    private Integer statusCodeToExceptionCode(StatusCode status) {
        Integer rcToRet = null;
        switch (status) {
            case EOK:
                rcToRet = BKException.Code.OK;
                break;
            case ENOENTRY:
                rcToRet = BKException.Code.NoSuchEntryException;
                break;
            case ENOLEDGER:
                rcToRet = BKException.Code.NoSuchLedgerExistsException;
                break;
            case EBADVERSION:
                rcToRet = BKException.Code.ProtocolVersionException;
                break;
            case ETRIMMED:
                rcToRet = BKException.Code.EntryTrimmedException;
                break;
            case EUA:
                rcToRet = BKException.Code.UnauthorizedAccessException;
                break;
            case EFENCED:
                rcToRet = BKException.Code.LedgerFencedException;
                break;
            case EREADONLY:
                rcToRet = BKException.Code.WriteOnReadOnlyBookieException;
                break;
            default:
                break;
        }
        return rcToRet;
    }

    private static StatusCode getStatusCodeFromErrorCode(int errorCode) {
        switch (errorCode) {
        case BookieProtocol.EOK:
            return StatusCode.EOK;
        case BookieProtocol.ENOLEDGER:
            return StatusCode.ENOLEDGER;
        case BookieProtocol.ENOENTRY:
            return StatusCode.ENOENTRY;
        case BookieProtocol.ETRIMMED:
            return StatusCode.ETRIMMED;
        case BookieProtocol.EBADREQ:
            return StatusCode.EBADREQ;
        case BookieProtocol.EIO:
            return StatusCode.EIO;
        case BookieProtocol.EUA:
            return StatusCode.EUA;
        case BookieProtocol.EBADVERSION:
            return StatusCode.EBADVERSION;
        case BookieProtocol.EFENCED:
            return StatusCode.EFENCED;
        case BookieProtocol.EREADONLY:
            return StatusCode.EREADONLY;
        default:
            throw new IllegalArgumentException("Invalid error code: " + errorCode);
        }
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }

}
