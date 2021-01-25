package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.server.MinimalServerConnectionManager;
import com.ibm.disni.RdmaEndpoint;

import java.net.InetAddress;
import java.net.InetSocketAddress;


/**
 * Manages the RDMA connections on behalf of an {@link RdmaServer}.
 */
public interface RdmaConnectionManager<T extends RdmaEndpoint> extends MinimalServerConnectionManager {

    /**
     * Returns the server that manages RDMA communications.
     * @return the server managing RDMA communications.
     */
    RdmaServer<T> getServer();


    /**
     * Starts accepting from and establishing connections to remote {@link RdmaEndpoint RdmaEndpoints}.
     */
    void start();


    /**
     * Attempts to stop any running tasks and remove all the connections.
     */
    void stopAndRemoveConnections();


    /**
     * Get a connection from a {@link InetAddress#toString()  InetAddress String}.
     * @param rdmaAddress the RDMA address to look for.
     * @return the connection requested or null if no such connection exists.
     */
    RdmaServerConnection getRdmaServerConnection(InetSocketAddress rdmaAddress);

    /**
     * Check if there is an RDMA connection towards this address.
     * @param address the address to check against.
     * @param isTcpAddress whether this is a TCP address. If so, the corresponding RDMA
     *                     address of the member will be retrieved, if it exists.
     *                     Otherwise, the provided address is considered an RDMA address.
     * @return true if there is an RDMA connection, false otherwise.
     */
    boolean isConnectedWithRdma(Address address, boolean isTcpAddress);
}
