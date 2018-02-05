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
package org.apache.bookkeeper.client.api;

import io.netty.channel.EventLoopGroup;
import io.netty.util.HashedWheelTimer;
import java.io.IOException;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * BookKeeper Client Builder to build client instances.
 *
 * @since 4.6
 */
@Public
@Unstable
public interface BookKeeperBuilder {

    /**
     * Configure the bookkeeper client with a provided Netty EventLoopGroup.
     *
     * @param eventLoopGroup an external {@link EventLoopGroup} to use by the bookkeeper client.
     *
     * @return client builder.
     */
    BookKeeperBuilder eventLoopGroup(EventLoopGroup eventLoopGroup);

    /**
     * Configure the bookkeeper client with a provided {@link StatsLogger}.
     *
     * @param statsLogger an {@link StatsLogger} to use by the bookkeeper client to collect stats generated by the
     * client.
     *
     * @return client builder.
     */
    BookKeeperBuilder statsLogger(StatsLogger statsLogger);

    /**
     * Configure the bookkeeper client to use the provided dns resolver {@link DNSToSwitchMapping}.
     *
     * @param dnsResolver dns resolver for placement policy to use for resolving network locations.
     *
     * @return client builder
     */
    BookKeeperBuilder dnsResolver(DNSToSwitchMapping dnsResolver);

    /**
     * Configure the bookkeeper client to use a provided Netty HashedWheelTimer.
     *
     * @param requestTimer request timer for client to manage timer related tasks.
     *
     * @return client builder
     */
    BookKeeperBuilder requestTimer(HashedWheelTimer requestTimer);

    /**
     * Configure the bookkeeper client to use a provided {@link FeatureProvider}.
     *
     * @param featureProvider the feature provider
     *
     * @return client builder
     */
    BookKeeperBuilder featureProvider(FeatureProvider featureProvider);

    /**
     * Start and initialize a new BookKeeper client.
     *
     * @return the client
     *
     * @throws BKException
     * @throws InterruptedException
     * @throws IOException
     */
    BookKeeper build() throws BKException, InterruptedException, IOException;

}
