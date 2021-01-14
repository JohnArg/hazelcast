package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.rdma.RdmaServerConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.verbs.IbvWC;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

public class NetRequestCompletionHandler extends AbstractWorkCompletionHandler {
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
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND
        if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            receiveProxy.releaseWorkRequest();
            // else if this is a completion for a RECV
        }else if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_RECV)) {
            PacketIOHelper packetIOHelper = new PacketIOHelper();
            Packet receivedPacket;
            // Todo - copying optimization
            // read the whole packet
            do{
                receivedPacket = packetIOHelper.readFrom(receiveProxy.getBuffer());
            }while (receivedPacket == null);
            // get the sender of the packet
            InetSocketAddress remoteAddress = null;
            ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) receiveProxy.getEndpoint();
            try {
                remoteAddress = (InetSocketAddress) communicator.getDstAddr();
                RdmaServerConnection connection = connectionManager.getRdmaServerConnection(remoteAddress.
                        getAddress().getHostAddress());
                if(connection == null){
                    throw new IOException("Cannot associate connection with received packet");
                }
                receivedPacket.setConn(connection);
            } catch (IOException e) {
                logger.severe(e);
                receiveProxy.releaseWorkRequest();
                return;
            }
            // must free up this work request, so that it can be reused for RDMA communications
            receiveProxy.releaseWorkRequest();
            connectionManager.onReceiveFromConnection(receivedPacket);

        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
            // associate event with a Work Request
            WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
            // Must free the request
            receiveProxy.releaseWorkRequest();
            // Status 5 can happen on remote side disconnect, since we have already posted
            // RECV requests for that remote side. We can simply close the remote endpoint
            // at this point.
            if(workCompletionEvent.getStatus() == IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()){
                ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) receiveProxy.getEndpoint();
                try {
                    communicator.close();
                } catch (IOException | InterruptedException e) {
                    logger.severe("Error in closing endpoint.", e);
                }
            }else{
                logger.severe("Error in network request "+ workCompletionEvent.getWr_id()
                        + " of status : " + workCompletionEvent.getStatus());
            }
    }
}