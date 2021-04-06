package com.hazelcast.internal.server.rdma.impl;


import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.jrcm.networking.dependencies.netbuffers.impl.TwoSidedBufferManager;
import jarg.jrcm.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.jrcm.networking.dependencies.svc.impl.TwoSidedSVCManager;

import java.io.IOException;

public class RdmaTwoSidedCommunicatorFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;
    private NodeEngine engine;
    private RdmaConnectionManagerImpl connectionManager;

    public RdmaTwoSidedCommunicatorFactory(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                           int maxBufferSize, int maxWorkRequests, NodeEngine engine,
                                           RdmaConnectionManagerImpl connectionManager) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWorkRequests = maxWorkRequests;
        this.engine = engine;
        this.connectionManager = connectionManager;
    }

    @Override
    public ActiveRdmaCommunicator createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        RdmaCommunicatorDependencies dependencies = new RdmaCommunicatorDependencies();
        dependencies
                .setMaxBufferSize(maxBufferSize)
                .setMaxWorkRequests(maxWorkRequests)
                .setBufferManager(new TwoSidedBufferManager(maxBufferSize, maxWorkRequests))
                .setProxyProvider(new QueuedProxyProvider(maxWorkRequests))
                .setSvcManager(new TwoSidedSVCManager(maxBufferSize, maxWorkRequests))
                .setWorkCompletionHandler(new NetRequestCompletionHandler(engine, connectionManager));
        
        return new ActiveRdmaCommunicator(endpointGroup, id, serverSide, dependencies);
    }
}
