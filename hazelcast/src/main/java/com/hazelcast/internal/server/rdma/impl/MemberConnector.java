package com.hazelcast.internal.server.rdma.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.rdma.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.RdmaServerConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import discovery.common.api.ServerIdentifier;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Connects to remote RDMA endpoints.
 */
public class MemberConnector implements Runnable{

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ConcurrentMap<InetSocketAddress, RdmaServerConnection> tcpToRdmaMap;
    ConcurrentMap<InetSocketAddress, RdmaServerConnection> rdmaAddressConnectionMap;
    private RdmaConfig rdmaConfig;
    private Collection<ServerIdentifier> membersToConnectTo;
    private ServerIdentifier localServerIdentifier;
    private NodeEngine engine;
    private RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager;
    private RdmaLogger logger;

    public MemberConnector(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                           ConcurrentMap<InetSocketAddress, RdmaServerConnection> tcpToRdmaMap,
                           ConcurrentMap<InetSocketAddress, RdmaServerConnection> rdmaAddressConnectionMap,
                           RdmaConfig rdmaConfig, Collection<ServerIdentifier> membersToConnectTo,
                           ServerIdentifier localServerIdentifier,
                           NodeEngine engine, RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager) {
        this.endpointGroup = endpointGroup;
        this.tcpToRdmaMap = tcpToRdmaMap;
        this.rdmaAddressConnectionMap = rdmaAddressConnectionMap;
        this.rdmaConfig = rdmaConfig;
        this.membersToConnectTo = membersToConnectTo;
        this.localServerIdentifier = localServerIdentifier;
        this.engine = engine;
        this.connectionManager = connectionManager;
        this.logger = new RdmaLogger(engine.getLogger(MemberConnector.class.getSimpleName()));
    }

    @Override
    public void run() {
        // read RDMA parameters
        int retries = rdmaConfig.getConnectionRetries();
        int retryDelay = rdmaConfig.getConnectionRetryDelay();
        int timeout = rdmaConfig.getTimeout();
        // connect to remote members
        for (ServerIdentifier server : membersToConnectTo) {
            ActiveRdmaCommunicator communicator = null;
            InetSocketAddress serverAddress = server.getRdmaAddress();
            RdmaServerConnection rdmaServerConnection = null;
            for(int trial=0; trial < retries; trial++) {
                try {
                    communicator = endpointGroup.createEndpoint();
                    communicator.connect(serverAddress, timeout);
                    rdmaServerConnection = new RdmaServerConnection(engine,communicator,
                            connectionManager, new Address(serverAddress), server);
                    break;
                } catch (Exception e) {
                    logger.severe("Cannot establish connection to address : " + serverAddress, e);
                    try {
                        communicator.close();
                    } catch (IOException | InterruptedException d) {
                        logger.severe("Cannot close communicator after connection failure.", d);
                    }
                }
                // we didn't connect, sleep and try again
                try {   // delay before retrying
                    Thread.sleep(retryDelay);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            if(rdmaServerConnection != null){
                tcpToRdmaMap.put(server.getTcpAddress(), rdmaServerConnection);
                rdmaAddressConnectionMap.put(server.getRdmaAddress(), rdmaServerConnection);
            }
        }
        // Tell the remote members you connected to who you are ===========================
        PacketIOHelper packetIOHelper = new PacketIOHelper();
        for(RdmaServerConnection serverConnection : tcpToRdmaMap.values()){
            ActiveRdmaCommunicator endpoint = serverConnection.getRdmaEndpoint();
            // Ask for an available Work Request from the Endpoint
            WorkRequestProxy workRequest = endpoint.getWorkRequestProxyProvider()
                    .getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
            // Get the buffer associated with the available work request
            ByteBuffer dataBuffer = workRequest.getBuffer();
            // create new Packet
            Packet packet = new Packet();
            packet.setPacketType(Packet.Type.SERVER_CONTROL);
            // write packet data to WR buffer
            packetIOHelper.writeTo(packet, dataBuffer);
            // Serialize the Server Identifier to the WR buffer as well
            localServerIdentifier.setWorkRequestProxy(workRequest);
            try {
                localServerIdentifier.writeToWorkRequestBuffer();
                dataBuffer.flip();
                // now we can send the data to the remote side
                workRequest.post();
            } catch (RpcDataSerializationException e) {
                workRequest.releaseWorkRequest();
                logger.severe("Cannot serialize server identifier. Message wasn't sent.", e);
            }
        }
    }
}
