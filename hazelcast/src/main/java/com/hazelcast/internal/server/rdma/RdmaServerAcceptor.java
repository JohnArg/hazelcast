package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.connections.RpcBasicEndpoint;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Will accept RDMA connections on a different thread and store
 * the client endpoints.
 */
public class RdmaServerAcceptor implements Runnable{

    private boolean terminate;
    private RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint;
    private Map<String, RpcBasicEndpoint> inboundConnections;
    private RdmaLogger logger;

    public RdmaServerAcceptor(NodeEngine engine,
                              RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint,
                              Map<String, RpcBasicEndpoint> inboundConnections){
        terminate = false;
        this.serverEndpoint = serverEndpoint;
        this.inboundConnections = inboundConnections;
        logger = new RdmaLogger(engine.getLogger(RdmaServerAcceptor.class));
    }

    @Override
    public void run() {
        // start server operation
        while(!terminate){
            try {
                RpcBasicEndpoint remoteEndpoint = serverEndpoint.accept();
                InetSocketAddress remoteAddress = (InetSocketAddress) remoteEndpoint.getDstAddr();
                inboundConnections.put(remoteAddress.getAddress().toString(),
                        remoteEndpoint);
                logger.info("Accepted the connection from " + remoteAddress.toString());
            }catch (IOException e) {
                logger.info("Server cannot accept client connections or has " +
                        "been interrupted.");
                terminate = true;
            }
        }
    }
}
