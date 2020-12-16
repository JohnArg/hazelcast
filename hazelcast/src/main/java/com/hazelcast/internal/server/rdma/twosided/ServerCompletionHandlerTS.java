package com.hazelcast.internal.server.rdma.twosided;

import com.ibm.disni.verbs.IbvWC;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import jarg.rdmarpc.connections.WorkCompletionHandler;

import java.nio.ByteBuffer;

public class ServerCompletionHandlerTS implements WorkCompletionHandler {

    @Override
    public void handleTwoSidedReceive(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint, ByteBuffer byteBuffer) {

    }

    @Override
    public void handleTwoSidedSend(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {

    }

    @Override
    public void handleOneSidedWrite(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {

    }

    @Override
    public void handleOneSidedRead(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {

    }
}
