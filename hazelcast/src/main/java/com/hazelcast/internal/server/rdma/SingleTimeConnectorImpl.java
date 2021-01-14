package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedEndpointFactory;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedServerConnectionManager;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;

import java.net.InetSocketAddress;
import java.util.Map;

public class SingleTimeConnectorImpl implements RemoteConnector {
    private RdmaLogger logger;
    private NodeEngine engine;
    RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> clientEndpointGroup;
    private RdmaTwoSidedEndpointFactory clientEndpointFactory;
    private Map<String, RdmaServerConnection> outboundConnections;
    private RdmaConfig settings;

    public SingleTimeConnectorImpl(NodeEngine engine, RdmaTwoSidedServerConnectionManager connectionManager,
                                   Map<String, RdmaServerConnection> outboundConnections,
                                   RdmaConfig settings) {
        this.logger = new RdmaLogger(engine.getLogger(RetryingConnectorImpl.class));
        this.engine = engine;
        this.connectionManager = connectionManager;
        this.outboundConnections = outboundConnections;
        this.settings = settings;
        // prepare a client endpoint group for connecting to other members
        try {
            clientEndpointGroup = new RdmaActiveEndpointGroup<>(settings.getTimeout(), settings.isPolling(),
                    settings.getMaxWRs(), settings.getMaxSge(), settings.getCqSize());
            clientEndpointFactory = new RdmaTwoSidedEndpointFactory(clientEndpointGroup,
                    settings.getMaxBufferSize(), settings.getMaxWRs(), engine, connectionManager);
            clientEndpointGroup.init(clientEndpointFactory);
        } catch (Exception e) {
            logger.severe(e);
        }
    }

    @Override
    public boolean connect(InetSocketAddress remoteAddress) {
        boolean success = false;
        try {
            // RDMA must connect to the same public remote ip as TCP, but in another port
            InetSocketAddress rdmaAddress = new InetSocketAddress(remoteAddress.getAddress(),
                    settings.getRdmaListeningPort());
            String rdmaAddressStr = rdmaAddress.getAddress().toString();
            // Try to connect up to certain number of retries, unless the thread is interrupted
            ActiveRdmaCommunicator remoteEndpoint = clientEndpointGroup.createEndpoint();
            try{
                remoteEndpoint.connect(rdmaAddress, settings.getTimeout());
                RdmaServerConnection serverConnection = new RdmaServerConnection(engine, remoteEndpoint,
                        connectionManager, new Address(rdmaAddress));
                outboundConnections.put(rdmaAddressStr, serverConnection);
                success = true;
                logger.info("Connected to server with address "+rdmaAddress);
            }catch (Exception e){
                logger.severe("Failed to connect to server with IP "+rdmaAddressStr + " .");
                remoteEndpoint.close();
            }
        } catch (Exception e) {
            logger.severe(e);
        }
        return success;
    }
}
