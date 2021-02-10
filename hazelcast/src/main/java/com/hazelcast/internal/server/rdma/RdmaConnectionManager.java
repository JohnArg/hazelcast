package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
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

    /**
     * Remove the {@link RdmaServerConnection} corresponding to the provided address.
     * @param address the address to check against.
     * @param isTcpAddress whether this is a TCP address or an RDMA address.
     *                     This clarification is required to find the corresponding {@link RdmaServerConnection},
     *                     since RDMA is used alongside TCP.
     */
    void removeConnection(Address address, boolean isTcpAddress);

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    default boolean transmit(Packet packet, Address target, String operationClassName) {
        return transmit(packet, target, 0, operationClassName);
    }

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @param streamId the stream id
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    boolean transmit(Packet packet, Address target, int streamId, String operationClassName);
}
