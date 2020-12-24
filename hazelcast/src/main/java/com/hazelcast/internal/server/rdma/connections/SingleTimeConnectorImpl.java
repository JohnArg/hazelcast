package com.hazelcast.internal.server.rdma.connections;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedEndpointFactory;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import jarg.rdmarpc.connections.WorkCompletionHandler;

import java.net.InetSocketAddress;
import java.util.Map;

public class SingleTimeConnectorImpl implements RdmaConnector{
    private RdmaLogger logger;
    private NodeEngine engine;
    RdmaConnectionManager<RpcBasicEndpoint> connectionManager;
    private RdmaActiveEndpointGroup<RpcBasicEndpoint> clientEndpointGroup;
    private RdmaTwoSidedEndpointFactory clientEndpointFactory;
    private Map<String, RdmaServerConnection> outboundConnections;
    private RdmaConfig settings;
    private WorkCompletionHandler netRequestCompletionHandler;

    public SingleTimeConnectorImpl(NodeEngine engine,RdmaConnectionManager<RpcBasicEndpoint> connectionManager,
                                   Map<String, RdmaServerConnection> outboundConnections,
                                   RdmaConfig settings, WorkCompletionHandler netRequestCompletionHandler) {
        this.logger = new RdmaLogger(engine.getLogger(RetryingConnectorImpl.class));
        this.engine = engine;
        this.connectionManager = connectionManager;
        this.outboundConnections = outboundConnections;
        this.settings = settings;
        this.netRequestCompletionHandler = netRequestCompletionHandler;
        // prepare a client endpoint group for connecting to other members
        try {
            clientEndpointGroup = new RdmaActiveEndpointGroup<>(settings.getTimeout(), settings.isPolling(),
                    settings.getMaxWRs(), settings.getMaxSge(), settings.getCqSize());
            clientEndpointFactory = new RdmaTwoSidedEndpointFactory(clientEndpointGroup,
                    netRequestCompletionHandler, settings.getMaxBufferSize(), settings.getMaxWRs());
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
            RpcBasicEndpoint remoteEndpoint = clientEndpointGroup.createEndpoint();
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
