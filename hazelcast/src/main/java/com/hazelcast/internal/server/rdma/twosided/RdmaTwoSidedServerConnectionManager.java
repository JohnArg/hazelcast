package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.networking.rdma.RdmaEndpointSettings;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.rdma.RdmaServerAcceptor;
import com.hazelcast.internal.server.rdma.RdmaServerConnector;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.connections.RpcBasicEndpoint;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;


/**
 * Manages the inbound RDMA connections on behalf of an RDMA server.
 */
public class RdmaTwoSidedServerConnectionManager implements ServerConnectionManager {

    private NodeEngine engine;
    private RdmaLogger logger;
    private InetSocketAddress localRdmaAddress;
    private RdmaTwoSidedServer server;
    private RdmaActiveEndpointGroup<RpcBasicEndpoint> serverEndpointGroup;
    private RdmaTwoSidedServerEndpointFactory serverEndpointFactory;
    private RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint;
    private Thread rdmaServerAcceptorTask;
    private FutureTask<Collection<CPMember>> rdmaServerConnectorTask;
    private Map<String, RpcBasicEndpoint> inboundConnections;
    private Map<String, RpcBasicEndpoint> outboundConnections;
    private RdmaEndpointSettings rdmaEndpointSettings;
    private Collection<CPMember> cpMembers;
    private CPMember localCPMember;


    public RdmaTwoSidedServerConnectionManager(NodeEngine engine,
                                               InetSocketAddress localRdmaAddress,
                                               RdmaTwoSidedServer server, RdmaEndpointSettings rdmaEndpointSettings) {
        this.engine = engine;
        this.logger = new RdmaLogger(engine.getLogger(RdmaTwoSidedServerConnectionManager.class));
        this.localRdmaAddress = localRdmaAddress;
        this.server = server;
        this.rdmaEndpointSettings = rdmaEndpointSettings;
        initializeConnectionDataStructures();
    }

    // Needed for resetting connections
    private void initializeConnectionDataStructures(){
        inboundConnections = new ConcurrentHashMap<>();
        outboundConnections = new HashMap<>();
    }

    /**
     * Will setup the server connections, but not start them. Must be called after discovering the CP members.
     */
    public void setupServerConnections(Collection<CPMember> cpMembers, CPMember localCPMember){
        this.cpMembers = cpMembers;
        this.localCPMember = localCPMember;
        try {
            // create the server endpoint that will accept rdma connections
            serverEndpointGroup = new RdmaActiveEndpointGroup<>(rdmaEndpointSettings.getTimeout(),
                    rdmaEndpointSettings.isPolling(), rdmaEndpointSettings.getMaxWRs(),
                    rdmaEndpointSettings.getMaxSge(), rdmaEndpointSettings.getCqSize());
            serverEndpointFactory = new RdmaTwoSidedServerEndpointFactory(serverEndpointGroup,
                    rdmaEndpointSettings.getMaxBufferSize(), rdmaEndpointSettings.getMaxWRs());
            serverEndpointGroup.init(serverEndpointFactory);
            serverEndpoint = serverEndpointGroup.createServerEndpoint();
            // bind server endpoint to address
            serverEndpoint.bind(localRdmaAddress, rdmaEndpointSettings.getServerBacklog());
            logger.info("server bound to address : " + localRdmaAddress.toString());
            // now we can prepare the tasks that will establish connections
            rdmaServerAcceptorTask = new Thread(new RdmaServerAcceptor(engine, serverEndpoint, inboundConnections));
            rdmaServerConnectorTask = new FutureTask<>(
                    new RdmaServerConnector(engine, cpMembers, outboundConnections,
                    rdmaEndpointSettings));
        } catch (Exception e) {
            logger.severe(e);
        }
    }

    public void startConnecting(){
        rdmaServerAcceptorTask.start();
        try {
            rdmaServerConnectorTask.run();
            Collection<CPMember> successfulConnections = (Collection<CPMember>) rdmaServerConnectorTask.get();
            // although we did not connect to ourselves, add this node for the sake of comparison
            successfulConnections.add(localCPMember);
            if(!successfulConnections.containsAll(cpMembers)){
                logger.severe("Could not establish connections towards all CP members.");
            }else{
                logger.info("Finished establishing connections towards all CP members.");
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.severe(e);
        } catch (CancellationException e){
            logger.info("Connecting to remote CP members canceled.");
        }
    }

    public void stopAndRemoveConnections(){
        if(rdmaServerAcceptorTask != null){
            rdmaServerAcceptorTask.interrupt();
        }
        if(rdmaServerConnectorTask != null){
            rdmaServerConnectorTask.cancel(true);
        }
        outboundConnections.values().forEach(endpoint -> {
            try {
                endpoint.close();
            } catch (IOException | InterruptedException e) {
                logger.severe(e);
            }
        });
        inboundConnections.values().forEach(endpoint -> {
            try {
                endpoint.close();
            } catch (IOException | InterruptedException e) {
                logger.severe(e);
            }
        });
        // reset the connection data structures (clearing them might be slower
        // than creating new objects, when having a lot of connections)
        initializeConnectionDataStructures();
    }

    /* *************************************************************************
     *   Overridden Methods
     * *************************************************************************/

    @Override
    public void addConnectionListener(ConnectionListener<ServerConnection> listener) {
        // Todo
    }

    @NotNull
    @Override
    public Collection<ServerConnection> getConnections() {
        return null;
    }

    @Override
    public boolean register(Address remoteAddress, ServerConnection connection, int planeIndex) {
        return false;
    }

    @Override
    public ServerConnection get(Address address, int streamId) {
        return null;
    }

    @Override
    public ServerConnection getOrConnect(Address address, int streamId) {
        return null;
    }

    @Override
    public ServerConnection getOrConnect(Address address, boolean silent, int streamId) {
        return null;
    }

    @Override
    public boolean transmit(Packet packet, Address target, int streamId) {
        // Todo
        return false;
    }

    @Override
    public NetworkStats getNetworkStats() {
        return null;
    }

    @Override
    public Server getServer() {
        return server;
    }

    @Override
    public void accept(Packet packet) {

    }

    /* *************************************************************************
    *   Getters / Setters
     * *************************************************************************/

    public RdmaServerEndpoint<RpcBasicEndpoint> getServerEndpoint() {
        return serverEndpoint;
    }

    public void setServerEndpoint(RdmaServerEndpoint<RpcBasicEndpoint> serverEndpoint) {
        this.serverEndpoint = serverEndpoint;
    }

    public RdmaEndpointSettings getRdmaEndpointSettings() {
        return rdmaEndpointSettings;
    }

    public void setRdmaEndpointSettings(RdmaEndpointSettings rdmaEndpointSettings) {
        this.rdmaEndpointSettings = rdmaEndpointSettings;
    }
}
