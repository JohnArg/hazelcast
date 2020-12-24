package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.RdmaServer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import org.jetbrains.annotations.NotNull;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;


public class RdmaTwoSidedServer implements RdmaServer<RpcBasicEndpoint> {
    private RdmaLogger logger;
    private Collection<CPMember> cpMembers;
    private Member localMember;
    private Address localAddress;
    private InetSocketAddress localRdmaAddress;
    private RdmaTwoSidedServerConnectionManager connectionManager;
    private AtomicBoolean isLive;
    private final InternalSerializationService serializationService;


    public RdmaTwoSidedServer(NodeEngine engine, Consumer<Packet> packetDispatcher,
                              InternalSerializationService serializationService,
                              RdmaConfig rdmaConfig) {
        logger = new RdmaLogger(engine.getLogger(RdmaTwoSidedServer.class));
        localMember = engine.getLocalMember();
        localAddress = localMember.getAddress();
        // Get a local RDMA address
        // RDMA must connect to the same public ip as TCP, but in its own port
        InetSocketAddress tcpInetAddress = localMember.getSocketAddress();
        InetAddress remoteIp = tcpInetAddress.getAddress();
        localRdmaAddress = new InetSocketAddress(remoteIp, rdmaConfig.getRdmaListeningPort());
        // the connection manager needs to be created before the server operation thread
        this.connectionManager = new RdmaTwoSidedServerConnectionManager(engine, packetDispatcher,
                localRdmaAddress, this,
                rdmaConfig);
        isLive = new AtomicBoolean(false);
        this.serializationService = serializationService;
    }

    @Override
    public RdmaConnectionManager<RpcBasicEndpoint> getConnectionManager() {
        return connectionManager;
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

    @NotNull
    @Override
    public Collection<RpcBasicEndpoint> getConnections() {
        return null;
    }

    public void setCpMembers(Collection<CPMember> cpMembers, CPMember localCPMember) {
        this.cpMembers = cpMembers;
        connectionManager.setupServerConnections(cpMembers, localCPMember);
    }

    public boolean send(Operation op, Address target){
        checkNotNull(target, "Target is required!");

        if (localAddress.equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }

        int streamId = op.getPartitionId();
        return connectionManager.transmit(toPacket(op), target, streamId);
    }

    private Packet toPacket(Operation op) {
        byte[] bytes = serializationService.toBytes(op);
        int partitionId = op.getPartitionId();
        Packet packet = new Packet(bytes, partitionId).setPacketType(Packet.Type.OPERATION);

        if (op.isUrgent()) {
            packet.raiseFlags(FLAG_URGENT);
        }
        return packet;
    }
}
