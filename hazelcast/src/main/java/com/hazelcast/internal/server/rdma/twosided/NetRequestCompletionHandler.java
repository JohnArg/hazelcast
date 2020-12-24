package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.RdmaServer;
import com.hazelcast.internal.server.rdma.connections.RdmaServerConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PacketDispatcher;
import com.ibm.disni.verbs.IbvWC;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import jarg.rdmarpc.connections.WorkCompletionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class NetRequestCompletionHandler implements WorkCompletionHandler {
    private NodeEngine nodeEngine;
    private RdmaLogger logger;
    private RdmaTwoSidedServerConnectionManager connectionManager;
    private Consumer<Packet> packetDispatcher;

    public NetRequestCompletionHandler(NodeEngine engine, RdmaTwoSidedServerConnectionManager connectionManager) {
        this.nodeEngine = engine;
        this.logger = new RdmaLogger(engine.getLogger(NetRequestCompletionHandler.class));
        this.connectionManager = connectionManager;
        this.packetDispatcher = connectionManager.getPacketDispatcher();
    }

    @Override
    public void handleTwoSidedReceive(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint, ByteBuffer byteBuffer) {
        // get the id of the work request that received a completion
        int wrId = (int) ibvWC.getWr_id();
        PacketIOHelper packetIOHelper = new PacketIOHelper();
        Packet receivedPacket;
        // Todo - copying optimization
        // read the whole packet
        do{
            receivedPacket = packetIOHelper.readFrom(byteBuffer);
        }while (receivedPacket == null);
        // get the sender of the packet
        InetSocketAddress remoteAddress = null;
        try {
            remoteAddress = (InetSocketAddress) rpcBasicEndpoint.getDstAddr();
            RdmaServerConnection connection = connectionManager.getRdmaServerConnection(remoteAddress.
                    getAddress().toString());
            if(connection == null){

                throw new IOException("Cannot associate connection with received packet");
            }
            receivedPacket.setConn(connection);
        } catch (IOException e) {
            logger.severe(e);
            return;
        }
        // must free up this work request, so that it can be reused for RDMA communications
        rpcBasicEndpoint.freeUpWrID(wrId, RpcBasicEndpoint.PostedRequestType.RECEIVE);
        connectionManager.onReceiveFromConnection(receivedPacket);
    }

    @Override
    public void handleTwoSidedSend(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {
        // get the id of the work request that received a completion
        int wrId = (int) ibvWC.getWr_id();
        // must free up this work request, so that it can be reused for RDMA communications
        rpcBasicEndpoint.freeUpWrID(wrId, RpcBasicEndpoint.PostedRequestType.SEND);
    }

    @Override
    public void handleOneSidedWrite(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleOneSidedRead(IbvWC ibvWC, RpcBasicEndpoint rpcBasicEndpoint) {
        throw new UnsupportedOperationException();
    }
}