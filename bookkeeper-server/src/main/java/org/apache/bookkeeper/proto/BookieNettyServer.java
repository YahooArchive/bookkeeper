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
package org.apache.bookkeeper.proto;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.auth.AuthProviderFactoryFactory;
import org.apache.bookkeeper.auth.BookieAuthProvider;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.processor.RequestProcessor;
import org.apache.commons.lang.SystemUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ExtensionRegistry;

/**
 * Netty server for serving bookie requests
 */
class BookieNettyServer {
    private final static Logger LOG = LoggerFactory.getLogger(BookieNettyServer.class);

    final static int maxMessageSize = PerChannelBookieClient.MAX_FRAME_LENGTH;
    final ServerConfiguration conf;
    final RequestProcessor requestProcessor;
    final ChannelGroup allChannels;
    final AtomicBoolean isRunning = new AtomicBoolean(false);
    Object suspensionLock = new Object();
    boolean suspended = false;

    final BookieAuthProvider.Factory authProviderFactory;
    final ExtensionRegistry registry = ExtensionRegistry.newInstance();

    final EventLoopGroup eventLoopGroup;

    BookieNettyServer(ServerConfiguration conf, RequestProcessor processor) throws IOException, KeeperException,
            InterruptedException, BookieException {
        this.conf = conf;
        this.requestProcessor = processor;
        this.authProviderFactory = AuthProviderFactoryFactory.newBookieAuthProviderFactory(conf, registry);

        ThreadFactory threadFactory = new DefaultThreadFactory("bookie-io");
        final int numThreads = Runtime.getRuntime().availableProcessors() * 2;

        EventLoopGroup eventLoopGroup;
        if (SystemUtils.IS_OS_LINUX) {
            try {
                eventLoopGroup = new EpollEventLoopGroup(numThreads, threadFactory);
            } catch (UnsatisfiedLinkError e) {
                eventLoopGroup = new NioEventLoopGroup(numThreads, threadFactory);
            }
        } else {
            eventLoopGroup = new NioEventLoopGroup(numThreads, threadFactory);
        }

        this.eventLoopGroup  = eventLoopGroup;
        allChannels = new CleanupChannelGroup(eventLoopGroup);

        InetSocketAddress bindAddress;
        if (conf.getListeningInterface() == null) {
            // listen on all interfaces
            bindAddress = new InetSocketAddress(conf.getBookiePort());
        } else {
            bindAddress = Bookie.getBookieAddress(conf).getSocketAddress();
        }
        listenOn(bindAddress);
    }

    boolean isRunning() {
        return isRunning.get();
    }

    @VisibleForTesting
    void suspendProcessing() {
        synchronized (suspensionLock) {
            suspended = true;
            for (Channel channel : allChannels) {
                channel.config().setAutoRead(false);
            }
        }
    }

    @VisibleForTesting
    void resumeProcessing() {
        synchronized (suspensionLock) {
            suspended = false;
            for (Channel channel : allChannels) {
                channel.config().setAutoRead(true);
            }
            suspensionLock.notifyAll();
        }
    }

    private void listenOn(InetSocketAddress address) throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.group(eventLoopGroup, eventLoopGroup);
        bootstrap.childOption(ChannelOption.TCP_NODELAY, conf.getServerTcpNoDelay());
        bootstrap.childOption(ChannelOption.SO_LINGER, 2);
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                new AdaptiveRecvByteBufAllocator(64 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024));

        if (eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollServerSocketChannel.class);
        } else {
            bootstrap.channel(NioServerSocketChannel.class);
        }

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                synchronized (suspensionLock) {
                    while (suspended) {
                        suspensionLock.wait();
                    }
                }

                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("lengthbaseddecoder", new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
                pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

                pipeline.addLast("bookieProtoDecoder", new BookieProtoEncoding.RequestDecoder(registry));
                pipeline.addLast("bookieProtoEncoder", new BookieProtoEncoding.ResponseEncoder(registry));
                pipeline.addLast("bookieAuthHandler", new AuthHandler.ServerSideHandler(authProviderFactory));

                ChannelInboundHandler requestHandler = isRunning.get() ? new BookieRequestHandler(conf,
                        requestProcessor, allChannels) : new RejectRequestHandler();

                pipeline.addLast("bookieRequestHandler", requestHandler);
            }
        });

        // Bind and start to accept incoming connections.
        bootstrap.bind(address.getAddress(), address.getPort()).sync();
    }

    void start() {
        isRunning.set(true);
    }

    void shutdown() {
        LOG.info("Shutting down BookieNettyServer");
        isRunning.set(false);
        allChannels.close().awaitUninterruptibly();
        eventLoopGroup.shutdownGracefully();
    }

    private static class RejectRequestHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            ctx.channel().close();
        }
    }

    private static class CleanupChannelGroup extends DefaultChannelGroup {
        private AtomicBoolean closed = new AtomicBoolean(false);

        CleanupChannelGroup(EventLoopGroup eventLoopGroup) {
            super("BookieChannelGroup", eventLoopGroup.next());
        }

        @Override
        public boolean add(Channel channel) {
            boolean ret = super.add(channel);
            if (closed.get()) {
                channel.close();
            }
            return ret;
        }

        @Override
        public ChannelGroupFuture close() {
            closed.set(true);
            return super.close();
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof CleanupChannelGroup)) {
                return false;
            }
            CleanupChannelGroup other = (CleanupChannelGroup) o;
            return other.closed.get() == closed.get() && super.equals(other);
        }

        @Override
        public int hashCode() {
            return super.hashCode() * 17 + (closed.get() ? 1 : 0);
        }
    }
}
