package com.hazelcast.internal.server;

import com.hazelcast.instance.EndpointQualifier;

/**
 * A very minimal interface for implementing servers. Since socket-based connections differ
 * from RDMA connections, this interface is meant to be extended according to the specific
 * needs of a socket-based or RDMA-based server.
 */
public interface MinimalServer {

    /**
     * Flag indicating the liveness status of the Server
     */
    boolean isLive();

    /**
     * Starts the Server, initializes its endpoints, starts threads, etc.
     * After start, Endpoints becomes fully operational.
     * <p>
     * If it is already started, then this method has no effect.
     *
     * @throws IllegalStateException if Server is shutdown
     */
    void start();

    /**
     * Stops the Server, releases its resources, stops threads, etc.
     * When stopped, is can be started again by calling {@link #start()}.
     * <p>
     * This method has no effect if it is already stopped or shutdown.
     * <p>
     * Currently {@code stop} is called during the merge process to detach node from the current cluster. After
     * node becomes ready to join to the new cluster, {@code start} is called.
     */
    void stop();

    /**
     * Shutdowns the Server completely.
     * Connections and the networking engine will not be operational anymore and cannot be restarted.
     * <p>
     * This method has no effect if it is already shutdown.
     */
    void shutdown();
}
