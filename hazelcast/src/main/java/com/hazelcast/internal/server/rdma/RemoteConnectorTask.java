package com.hazelcast.internal.server.rdma;


import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Wraps an {@link RemoteConnector} into a Callable that can be run in another thread.
 */
public class RemoteConnectorTask implements Callable<Collection<InetSocketAddress>> {
    private RemoteConnector rdmaConnector;
    private List<InetSocketAddress> remoteAddresses;

    public RemoteConnectorTask(RemoteConnector rdmaConnector, List<InetSocketAddress> remoteAddresses){
        this.rdmaConnector = rdmaConnector;
        this.remoteAddresses = remoteAddresses;
    }

    @Override
    public Collection<InetSocketAddress> call() {
        return rdmaConnector.connect(remoteAddresses);
    }

}
