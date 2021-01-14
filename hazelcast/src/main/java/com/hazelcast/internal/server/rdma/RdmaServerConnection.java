package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.OutboundFrame;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.spi.impl.NodeEngine;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class RdmaServerConnection implements ServerConnection {
    private final String CONNECTION_TYPE = ConnectionType.MEMBER;
    private RdmaLogger logger;
    private ActiveRdmaCommunicator rdmaEndpoint;
    private ConcurrentMap<String, String> attributeMap;
    private RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager;
    private boolean isAlive;
    private String closeReason;
    private Throwable closeCause;
    private Address remoteAddress;

    public RdmaServerConnection(NodeEngine engine, ActiveRdmaCommunicator rdmaEndpoint,
                                RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager,
                                Address remoteAddress){
        this.logger = new RdmaLogger(engine.getLogger(RdmaServerConnection.class));
        this.rdmaEndpoint = rdmaEndpoint;
        this.connectionManager = connectionManager;
        this.attributeMap = new ConcurrentHashMap<>();
        this.isAlive = true;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public ConcurrentMap attributeMap() {
        return attributeMap;
    }

    @Override
    public boolean isAlive() {
        return isAlive;
    }

    @Override
    public long lastReadTimeMillis() {
        return 0;
    }

    @Override
    public long lastWriteTimeMillis() {
        return 0;
    }

    @Override
    public InetSocketAddress getRemoteSocketAddress() {
        InetSocketAddress address = null;
        try{
            address = remoteAddress.getInetSocketAddress();
        }catch (IOException e) {
            logger.severe(e);
        }
        return address;
    }

    @Override
    public Address getRemoteAddress() {
        return remoteAddress;
    }

    @Override
    public void setRemoteAddress(Address remoteAddress) { this.remoteAddress = remoteAddress; }

    @Override
    public InetAddress getInetAddress() {
        InetAddress remoteIp = null;
        try{
            remoteIp = remoteAddress.getInetAddress();
        }catch (IOException e) {
            logger.severe(e);
            return null;
        }
        return remoteIp;
    }

    @Override
    public boolean write(OutboundFrame frame) {
        return connectionManager.transmit((Packet) frame, getRemoteAddress());
    }

    @Override
    public void close(String reason, Throwable cause) {
        try {
            rdmaEndpoint.close();
            closeReason = reason;
            closeCause = cause;
            logger.info("Closing RDMA server connection. Reason : "+reason);
        } catch (IOException | InterruptedException e) {
            logger.severe(e);
        }
    }

    @Override
    public String getCloseReason() {
        return closeReason;
    }

    @Override
    public Throwable getCloseCause() {
        return closeCause;
    }

    /**
     * Not applicable in this class. Use {@link RdmaServerConnection#getRdmaConnectionManager()} instead.
     * @return the connection manager.
     */
    @Override
    public ServerConnectionManager getConnectionManager() {
        return null;
    }

    public RdmaConnectionManager<ActiveRdmaCommunicator> getRdmaConnectionManager(){
        return connectionManager;
    }

    @Override
    public String getConnectionType() {
        return CONNECTION_TYPE;
    }

    @Override
    public void setConnectionType(String connectionType) {

    }

    @Override
    public boolean isClient() {
        return false;
    }

    public ActiveRdmaCommunicator getRdmaEndpoint() {
        return rdmaEndpoint;
    }
}
