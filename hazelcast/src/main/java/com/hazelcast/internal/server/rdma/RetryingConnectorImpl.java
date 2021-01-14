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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RetryingConnectorImpl implements RemoteConnector {
    private RdmaLogger logger;
    private NodeEngine engine;
    RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> clientEndpointGroup;
    private RdmaTwoSidedEndpointFactory clientEndpointFactory;
    private Map<String, RdmaServerConnection> outboundConnections;
    private RdmaConfig settings;

    public RetryingConnectorImpl(NodeEngine engine, RdmaTwoSidedServerConnectionManager connectionManager,
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
            String rdmaAddressStr = rdmaAddress.getAddress().getHostAddress();
            // Try to connect up to certain number of retries, unless the thread is interrupted
            ActiveRdmaCommunicator remoteEndpoint;
            int retryDelay = settings.getConnectionRetryDelay();

            for(int retries = settings.getConnectionRetries(); retries > 0; retries --){
                // allow for canceling the operation when running in another thread
                if(Thread.interrupted()){
                    return false;
                }
                remoteEndpoint = clientEndpointGroup.createEndpoint();
                try{
                    remoteEndpoint.connect(rdmaAddress, settings.getTimeout());
                    RdmaServerConnection serverConnection = new RdmaServerConnection(engine, remoteEndpoint,
                            connectionManager, new Address(rdmaAddress));
                    outboundConnections.put(rdmaAddressStr, serverConnection);
                    success = true;
                    logger.info("Connected to server with address "+rdmaAddress);
                    break;
                }catch (Exception e){
                    //logger.severe(e);
                    remoteEndpoint.close();
                    Thread.sleep(retryDelay);
                }
            }
            if(!success){
                logger.severe("Failed to connect to server with IP "+rdmaAddressStr + " " +
                        "after "+ settings.getConnectionRetries() + " retries");
            }
        } catch (Exception e) {
            logger.severe(e);
        }
        return success;
    }

    @Override
    public List<InetSocketAddress> connect(List<InetSocketAddress> remoteAddresses){
        boolean success;
        List<InetSocketAddress> failedAddresses = new ArrayList<>();
        InetSocketAddress remoteAddress;

        for(int i=0; i <  remoteAddresses.size(); i++){
            // Allow the interruption of this processing
            if(Thread.interrupted()){
                if(i == 0){
                    return remoteAddresses;
                }
                failedAddresses.addAll(remoteAddresses.subList(i, remoteAddresses.size()));
                return failedAddresses;
            }
            remoteAddress = remoteAddresses.get(i);
            success = connect(remoteAddress);
            if(!success){
                failedAddresses.add(remoteAddress);
            }
        }
        return failedAddresses;
    }
}
