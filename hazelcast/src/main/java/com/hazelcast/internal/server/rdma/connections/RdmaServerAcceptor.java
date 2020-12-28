package com.hazelcast.internal.server.rdma.connections;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.connections.RpcBasicEndpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

/**
 * Will accept RDMA connections on a different thread and store
 * the client endpoints.
 */
public class RdmaServerAcceptor implements Runnable{

    private boolean terminate;
    private RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint;
    private Map<String, RdmaServerConnection> inboundConnections;
    private NodeEngine engine;
    RdmaConnectionManager<RpcBasicEndpoint> connectionManager;
    private RdmaLogger logger;

    public RdmaServerAcceptor(NodeEngine engine, RdmaConnectionManager<RpcBasicEndpoint> connectionManager,
                              RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint,
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
                RpcBasicEndpoint remoteEndpoint = serverEndpoint.accept();
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
