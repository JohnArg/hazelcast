package com.hazelcast.internal.server.rdma.twosided;


import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.rdmarpc.networking.dependencies.netbuffers.impl.TwoSidedBufferManager;
import jarg.rdmarpc.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.rdmarpc.networking.dependencies.svc.impl.TwoSidedSVCManager;

import java.io.IOException;

public class RdmaTwoSidedEndpointFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;
    private NodeEngine engine;
    private RdmaTwoSidedServerConnectionManager connectionManager;

    public RdmaTwoSidedEndpointFactory(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                       int maxBufferSize, int maxWorkRequests, NodeEngine engine,
                                       RdmaTwoSidedServerConnectionManager connectionManager) {
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
