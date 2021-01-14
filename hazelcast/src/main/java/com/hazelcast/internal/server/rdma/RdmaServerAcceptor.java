package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Will accept RDMA connections on a different thread and store
 * the client endpoints.
 */
public class RdmaServerAcceptor implements Runnable{

    private boolean terminate;
    private RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private Map<String, RdmaServerConnection> inboundConnections;
    private NodeEngine engine;
    RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager;
    private RdmaLogger logger;

    public RdmaServerAcceptor(NodeEngine engine, RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager,
                              RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint,
                              Map<String, RdmaServerConnection> inboundConnections){
        this.terminate = false;
        this.serverEndpoint = serverEndpoint;
        this.inboundConnections = inboundConnections;
        this.engine = engine;
        this.connectionManager = connectionManager;
        this.logger = new RdmaLogger(engine.getLogger(RdmaServerAcceptor.class));
    }

    @Override
    public void run() {
        // start server operation
        while(!terminate){
            try {
                ActiveRdmaCommunicator remoteEndpoint = serverEndpoint.accept();
                InetSocketAddress remoteAddress = (InetSocketAddress) remoteEndpoint.getDstAddr();
                RdmaServerConnection serverConnection = new RdmaServerConnection(engine, remoteEndpoint,
                        connectionManager, new Address(remoteAddress));
                inboundConnections.put(remoteAddress.getAddress().getHostAddress(), serverConnection);
                logger.info("Accepted the connection from " + remoteAddress.toString());
            }catch (IOException e) {
                logger.info("Server cannot accept client connections or has " +
                        "been interrupted.");
                terminate = true;
            }
        }
    }
}
