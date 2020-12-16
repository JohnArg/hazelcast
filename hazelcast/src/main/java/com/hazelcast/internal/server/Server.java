/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.server;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.ConnectionListenable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The Server is responsible for managing {@link ServerConnection} instances.
 *
 * Given an {@link EndpointQualifier} an {@link ServerConnectionManager} can be retrieved
 * by {@link #getConnectionManager(EndpointQualifier)} to create or get connections on that end.
 */
public interface Server extends ConnectionListenable<ServerConnection>, MinimalServer {

    /**
     * Returns the ServerContext.
     */
    ServerContext getContext();

    /**
     * Returns a {@link ServerConnectionManager} that is able to handle
     * connections for the given qualifier.
     *
     * It is important to realize that the returned manager could also
     * manage other connections; so if you ask for a member connection manager
     * it could be that you get a connection manager that also handles client
     * connections. So if you would ask for {@link ServerConnectionManager#getConnections()}
     * it could contain a mixture of client and member connections.
     *
     * @param qualifier the EndpointQualifier used to select the right connection manager.
     * @return the relevant {@link ServerConnectionManager} that manages connections
     * for the given an {@link EndpointQualifier}. For {@code EndpointQualifier#MEMBER}
     * there will always be an ServerConnectionManager, but for the other ones null could be returned.
     */
    ServerConnectionManager getConnectionManager(EndpointQualifier qualifier);

    /**
     * Returns all connections.
     *
     * This can be a relatively expensive operations the returned collection might be created
     * on every invocation. So if you are just interested in count, have a look at
     * {@link #connectionCount(Predicate)} method.
     *
     * @return the connections.
     */
    @Nonnull
    Collection<ServerConnection> getConnections();

    /**
     * Counts the number of connections satisfying some predicate.
     *
     * @param predicate the Predicate. Predicate can be null which means that no filtering is done.
     * @return the number of connections
     */
    default int connectionCount(@Nullable Predicate<ServerConnection> predicate) {
        // a default implementation is provided for testing purposes.

        if (predicate == null) {
            return getConnections().size();
        }

        return (int) getConnections().stream().filter(predicate).count();
    }

    /***
     * Counts the number of connections.
     *
     * @return number of connections.
     */
    default int connectionCount() {
        return connectionCount(null);
    }

    /**
     * Returns network stats for inbound and outbound traffic per {@link EndpointQualifier}.
     * Stats are available only when Advanced Networking is enabled.
     *
     * @return network stats per endpoint
     */
    Map<EndpointQualifier, NetworkStats> getNetworkStats();

}
