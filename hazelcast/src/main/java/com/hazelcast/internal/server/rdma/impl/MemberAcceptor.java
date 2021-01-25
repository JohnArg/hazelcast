package com.hazelcast.internal.server.rdma.impl;

import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.rdma.RdmaConnectionManager;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaServerEndpoint;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;

/**
 * Accepts and stores RDMA connections.
 */
public class MemberAcceptor<T extends RdmaEndpoint> implements Runnable{

    private boolean terminate;
    private RdmaServerEndpoint<T> serverEndpoint;
    private ConcurrentMap<InetSocketAddress, T> inboundConnections;
    private RdmaConnectionManager<T> connectionManager;
    private RdmaLogger logger;

    public MemberAcceptor(NodeEngine engine, RdmaServerEndpoint<T> serverEndpoint,
                          ConcurrentMap<InetSocketAddress, T> inboundConnections,
                          RdmaConnectionManager<T> connectionManager) {
        this.terminate = false;
        this.serverEndpoint = serverEndpoint;
        this.inboundConnections = inboundConnections;
        this.connectionManager = connectionManager;
        this.logger = new RdmaLogger(engine.getLogger(MemberAcceptor.class.getSimpleName()));
    }

    @Override
    public void run() {
        // start server operation
        while(!terminate){
            try {
                T remoteEndpoint = serverEndpoint.accept();
                InetSocketAddress remoteAddress = (InetSocketAddress) remoteEndpoint.getDstAddr();
                inboundConnections.put(remoteAddress, remoteEndpoint);
                logger.info("Accepted the connection from " + remoteAddress.toString());
            }catch (IOException e) {
                logger.info("Server cannot accept client connections or has " +
                        "been interrupted.");
                terminate = true;
            }
        }
        try {
            serverEndpoint.close();
        } catch (IOException | InterruptedException e) {
            logger.severe("Cannot close the connections accepting server endpoint.", e);
        }
    }
}
