package com.hazelcast.internal.server.rdma.connections;

import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedEndpointFactory;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import jarg.rdmarpc.connections.WorkCompletionHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Wraps an {@link RdmaConnector} into a Callable that can be run in another thread.
 */
public class RdmaConnectorTask implements Callable<Collection<InetSocketAddress>> {
    private RdmaConnector rdmaConnector;
    private List<InetSocketAddress> remoteAddresses;

    public RdmaConnectorTask(RdmaConnector rdmaConnector,  List<InetSocketAddress> remoteAddresses){
        this.rdmaConnector = rdmaConnector;
        this.remoteAddresses = remoteAddresses;
    }

    @Override
    public Collection<InetSocketAddress> call() {
        return rdmaConnector.connect(remoteAddresses);
    }

}
