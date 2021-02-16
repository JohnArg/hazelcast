package com.hazelcast.internal.server.rdma.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.rdma.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.RdmaServer;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Implementation of an {@link RdmaServer}.
 */
public class RdmaServerImpl implements RdmaServer<ActiveRdmaCommunicator> {
    private RdmaLogger logger;
    private Member localMember;
    private RdmaConnectionManagerImpl connectionManager;
    private AtomicBoolean isLive;
    private final InternalSerializationService serializationService;


    public RdmaServerImpl(NodeEngine engine, Member localMember, Consumer<Packet> packetDispatcher,
                          InternalSerializationService serializationService,
                          RdmaConfig rdmaConfig) {
        logger = new RdmaLogger(engine.getLogger(RdmaServerImpl.class.getSimpleName()));
        this.localMember = localMember;
        // the connection manager needs to be created before the server operation thread
        this.connectionManager = new RdmaConnectionManagerImpl(engine, packetDispatcher,
                this, rdmaConfig);
        isLive = new AtomicBoolean(false);
        this.serializationService = serializationService;
    }

    @Override
    public RdmaConnectionManager<ActiveRdmaCommunicator> getConnectionManager() {
        return connectionManager;
    }

    @Override
    public boolean isLive() {
        return isLive.get();
    }

    @Override
    public void start() {
        logger.info("Starting RDMA server operation.");
        connectionManager.start();
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
    public Collection<ActiveRdmaCommunicator> getConnections() {
        return null;
    }


    public boolean send(Operation op, Address target){
        checkNotNull(target, "Target is required!");
        if (localMember.getAddress().equals(target)) {
            throw new IllegalArgumentException("Target is this node! -> " + target + ", op: " + op);
        }
        int streamId = op.getPartitionId();
        return connectionManager.transmit(toPacket(op), target, streamId);
    }

    // Todo - serialization copy optimization
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