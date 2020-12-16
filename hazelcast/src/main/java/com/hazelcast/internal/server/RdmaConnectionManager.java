package com.hazelcast.internal.server;

import com.hazelcast.cp.CPMember;
import com.ibm.disni.RdmaEndpoint;

import java.util.Collection;



public interface RdmaConnectionManager<T extends RdmaEndpoint> {

    /**
     * Returns the server that manages RDMA communications.
     * @return the server managing RDMA communications.
     */
    RdmaServer<T> getServer();

    /**
     * Will setup the server connections, but not start them. Must be called after discovering the CP members.
     */
    void setupServerConnections(Collection<CPMember> cpMembers, CPMember localCPMember);


    /**
     * Starts accepting from and establishing connections to remote {@link RdmaEndpoint RdmaEndpoints}.
     * Must be called after {@link RdmaConnectionManager#setupServerConnections(Collection, CPMember)}.
     */
    void startConnecting();


    /**
     * Attempts to stop any running tasks and remove all the connections.
     */
    void stopAndRemoveConnections();
}
