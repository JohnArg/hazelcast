package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.rdma.RdmaEndpointSettings;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.*;
import com.hazelcast.spi.impl.NodeEngine;
import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;


public class RdmaTwoSidedServer implements Server {
    private RdmaLogger logger;
    private Collection<CPMember> cpMembers;
    private Member localMember;
    private InetSocketAddress localRdmaAddress;
    private RdmaEndpointSettings rdmaEndpointSettings;
    private RdmaTwoSidedServerConnectionManager connectionManager;
    private AtomicBoolean isLive;


    public RdmaTwoSidedServer(NodeEngine engine, RdmaEndpointSettings rdmaEndpointSettings) {
        logger = new RdmaLogger(engine.getLogger(RdmaTwoSidedServer.class));
        this.localMember = engine.getLocalMember();

        // Get a local RDMA address
        // RDMA must connect to the same public ip as TCP, but in its own port
        InetSocketAddress tcpInetAddress = localMember.getSocketAddress();
        InetAddress remoteIp = tcpInetAddress.getAddress();
        localRdmaAddress = new InetSocketAddress(remoteIp, rdmaEndpointSettings.getRdmaListeningPort());

        this.rdmaEndpointSettings = rdmaEndpointSettings;
        // the connection manager needs to be created before the server operation thread
        this.connectionManager = new RdmaTwoSidedServerConnectionManager(engine, localRdmaAddress, this,
                rdmaEndpointSettings);
        isLive = new AtomicBoolean(false);
    }

    @Override
    public void addConnectionListener(ConnectionListener<ServerConnection> listener) {
        // Todo
    }

    @Override
    public ServerContext getContext() {
        return null;
    }

    @Override
    public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        return connectionManager;
    }

    @NotNull
    @Override
    public Collection<ServerConnection> getConnections() {
        return connectionManager.getConnections();
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        return null;
    }

    @Override
    public boolean isLive() {
        return isLive.get();
    }

    @Override
    public void start() {
        logger.info("Starting RDMA server operation.");
        connectionManager.startConnecting();
        isLive.set(true);
    }

    @Override
    public void stop() {
        logger.info("Stopping RDMA server operation.");
        if(connectionManager != null){
            connectionManager.stopAndRemoveConnections();
        }
        isLive.set(false);
    }

    @Override
    public void shutdown() {
        logger.info("RDMA server will be shut down");
        stop();
    }

    public void setCpMembers(Collection<CPMember> cpMembers, CPMember localCPMember) {
        this.cpMembers = cpMembers;
        connectionManager.setupServerConnections(cpMembers, localCPMember);
    }
}
