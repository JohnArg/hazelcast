package com.hazelcast.internal.server.rdma.twosided;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import jarg.rdmarpc.requests.WorkRequestTypes;

import java.io.IOException;

public class RdmaTwoSidedServerEndpointFactory implements RdmaEndpointFactory<RpcBasicEndpoint> {
    private RdmaActiveEndpointGroup<RpcBasicEndpoint> endpointGroup;
    private int maxBufferSize;
    private int maxWRs;

    public RdmaTwoSidedServerEndpointFactory(RdmaActiveEndpointGroup<RpcBasicEndpoint> endpointGroup,
                                             int maxBufferSize, int maxWRs) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWRs = maxWRs;
    }

    @Override
    public RpcBasicEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        return new RpcBasicEndpoint(endpointGroup, id, serverSide, maxBufferSize, maxWRs,
                WorkRequestTypes.TWO_SIDED_SIGNALED, new ServerCompletionHandlerTS());
    }
}

