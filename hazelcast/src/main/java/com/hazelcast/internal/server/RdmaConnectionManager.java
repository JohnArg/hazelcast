package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.server.rdma.RdmaServerConnection;
import com.ibm.disni.RdmaEndpoint;

import java.net.InetAddress;
import java.util.Collection;



public interface RdmaConnectionManager<T extends RdmaEndpoint> extends MinimalServerConnectionManager{

    /**
     * Returns the server that manages RDMA communications.
     * @return the server managing RDMA communications.
     */
    RdmaServer<T> getServer();

    /**
     * Will setup RDMA communications, but not start them.
     * @return true on success, false on failure.
     */
    boolean initializeRdmaCommunications(Collection<CPMember> cpMembers, CPMember localCPMember);


    /**
     * Starts accepting from and establishing connections to remote {@link RdmaEndpoint RdmaEndpoints}.
     * Must be called after {@link RdmaConnectionManager#initializeRdmaCommunications(Collection, CPMember)}.
     */
    void startConnecting();


    /**
     * Attempts to stop any running tasks and remove all the connections.
     */
    void stopAndRemoveConnections();


    /**
     * Get a connection from a {@link InetAddress#toString()  InetAddress String}.
     * @param inetAddressStr the {@link InetAddress} String.
     * @return the connection requested or null if no such connection exists.
     */
    RdmaServerConnection getRdmaServerConnection(String inetAddressStr);

    /**
     * Check if there is an RDMA connection towards this address.
     * @param address the address to check against.
     * @return true if there is an RDMA connection, false otherwise.
     */
    boolean isConnectedWithRdma(Address address);
}
