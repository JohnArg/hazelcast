package com.hazelcast.internal.server.rdma.impl;

import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.rdma.RdmaServerConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.verbs.IbvWC;
import discovery.common.api.ServerIdentifier;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxyProvider;
import jarg.rdmarpc.networking.dependencies.netrequests.types.PostedRequestType;
import jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

public class NetRequestCompletionHandler extends AbstractWorkCompletionHandler {
    private NodeEngine nodeEngine;
    private RdmaLogger logger;
    private RdmaConnectionManagerImpl connectionManager;
    private Consumer<Packet> packetDispatcher;

    public NetRequestCompletionHandler(NodeEngine engine, RdmaConnectionManagerImpl connectionManager) {
        this.nodeEngine = engine;
        this.logger = new RdmaLogger(engine.getLogger(NetRequestCompletionHandler.class));
        this.connectionManager = connectionManager;
        this.packetDispatcher = connectionManager.getPacketDispatcher();
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = proxyProvider.getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND ================================================
        if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            receiveProxy.releaseWorkRequest();
            // else if this is a completion for a RECV ======================================
        }else if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_RECV)) {
            // get details from packet/connection
            PacketIOHelper packetIOHelper = new PacketIOHelper();
            Packet receivedPacket;
            ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) receiveProxy.getRdmaCommunicator();
            InetSocketAddress senderAddress = null;
            try {
                senderAddress = (InetSocketAddress) communicator.getDstAddr();
            }catch (IOException e){
                receiveProxy.releaseWorkRequest();
                logger.severe("Cannot retrieve socket address from communicator.", e);
                return;
            }
            // Sent Data => Packet
            do{
                receivedPacket = packetIOHelper.readFrom(receiveProxy.getBuffer());
            }while (receivedPacket == null);

            // Handle Packet =======================================
            if(receivedPacket.getPacketType().equals(Packet.Type.SERVER_CONTROL)){
                ServerIdentifier serverIdentifier = new ServerIdentifier();
                serverIdentifier.setWorkRequestProxy(receiveProxy);
                try {
                    serverIdentifier.readFromWorkRequestBuffer();
                    // must free up this work request, so that it can be reused for RDMA communications
                    receiveProxy.releaseWorkRequest();
                    connectionManager.handleServerRegistration(serverIdentifier, communicator, senderAddress);
                } catch (RpcDataSerializationException e) {
                    receiveProxy.releaseWorkRequest();
                    logger.severe("Cannot deserialize Server Identifier from received packet.", e);
                }
            }else {
                try {
                    RdmaServerConnection connection = connectionManager.getRdmaServerConnection(senderAddress);
                    if(connection == null){
                        throw new IOException("Cannot associate connection with received packet. Sender : " +
                                senderAddress);
                    }
                    receivedPacket.setConn(connection);
                } catch (IOException e) {
                    receiveProxy.releaseWorkRequest();
                    logger.severe(e);
                    return;
                }
                // must free up this work request, so that it can be reused for RDMA communications
                receiveProxy.releaseWorkRequest();
                connectionManager.onReceiveFromConnection(receivedPacket);
            }
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = proxyProvider.getWorkRequestProxyForWc(workCompletionEvent);
        ActiveRdmaCommunicator rdmaCommunicator = (ActiveRdmaCommunicator) workRequestProxy.getRdmaCommunicator();
        /*
         *   Print errors when the communications are ongoing. When they are shut down, we expect
         *   IBV_WC_WR_FLUSH_ERR errors to occur, since we have pre-posted RECVs to the NIC that will be
         *   flushed, which is OK. Any other errors events occurring after shutting down the communications
         *   will also be ignored.
         */
        if(rdmaCommunicator.isShutDown()){
            return;
        }

        if((workCompletionEvent.getStatus() != IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()) ||
                !(workRequestProxy.getPostType().equals(PostedRequestType.RECEIVE))){
            // prepare the message to sent
            InetSocketAddress endpointAddress = null;
            try {
                endpointAddress = (InetSocketAddress) rdmaCommunicator.getDstAddr();
            } catch (IOException e) {
                //ignore
            }
            StringBuilder messageBuilder = new StringBuilder("Error : {");
            messageBuilder.append("WR id : "+ workCompletionEvent.getWr_id()+", ");
            messageBuilder.append("WC status : " + workCompletionEvent.getStatus() + ", ");
            messageBuilder.append("WC opcode : " + workCompletionEvent.getOpcode() + ", ");
            messageBuilder.append("WR type : ");
            // following might be null
            messageBuilder.append(workRequestProxy.getWorkRequestType());
            if(endpointAddress != null){
                messageBuilder.append(", address : " + endpointAddress +"}");
            }else{
                messageBuilder.append("}");
            }
            if(workCompletionEvent.getStatus() == IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()){
                logger.warning(messageBuilder.toString());
            }else{
                logger.severe(messageBuilder.toString());
            }
        }
        // must free the request if the communicator is not shut down
        workRequestProxy.releaseWorkRequest();
    }
}