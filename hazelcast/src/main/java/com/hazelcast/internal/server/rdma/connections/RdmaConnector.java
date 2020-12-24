package com.hazelcast.internal.server.rdma.connections;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs connections to remote RDMA servers. Implementations can define their own
 * connection strategy.
 */
public interface RdmaConnector{

    /**
     * Connects to a remote address.
     * @param remoteAddress the address to connect to.
     * @return true on success, false on failure.
     */
    boolean connect(InetSocketAddress remoteAddress);

    /**
     * Will attempt to connect to a list of remote addresses.
     * It returns a list of the addresses it failed to connect to.
     * @param remoteAddresses the list of addresses to connect to.
     * @return the list of addresses towards which a connection could not be established.
     * If the list is empty, then all connections were successful.
     */
    default List<InetSocketAddress> connect(List<InetSocketAddress> remoteAddresses){
        boolean success;
        List<InetSocketAddress> failedAddresses = new ArrayList<>();

        for(InetSocketAddress remoteAddress : remoteAddresses){
            success = connect(remoteAddress);
            if(!success){
                failedAddresses.add(remoteAddress);
            }
        }
        return failedAddresses;
    }
}
