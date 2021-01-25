package com.hazelcast.internal.server.rdma;

import com.hazelcast.internal.server.MinimalServer;
import com.hazelcast.internal.server.Server;
import com.ibm.disni.RdmaEndpoint;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * The server that manages RDMA communications. Similar to the {@link Server} interface, which manages socket-type
 * communications.
 *
 * @param <C> the type of RDMA endpoints used for communications.
 */
public interface RdmaServer<C extends RdmaEndpoint> extends MinimalServer {

    /**
     * Returns all RDMA connections.
     *
     * @return the connections.
     */
    @Nonnull
    Collection<C> getConnections();


    /**
     * Returns the RDMA connection manager.
     * @return the RDMA connection manager.
     */
    RdmaConnectionManager<C> getConnectionManager();

    /**
     * Counts the number of connections satisfying some predicate.
     *
     * @param predicate the Predicate. Predicate can be null which means that no filtering is done.
     * @return the number of connections
     */
    default int connectionCount(@Nullable Predicate<C> predicate) {
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
}
