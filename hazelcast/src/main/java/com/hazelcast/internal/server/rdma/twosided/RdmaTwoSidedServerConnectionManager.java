package com.hazelcast.internal.server.rdma.twosided;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.RdmaService;
import com.hazelcast.internal.networking.rdma.RdmaServiceState;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.RdmaServer;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.rdma.*;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Manages the inbound RDMA connections on behalf of an RDMA server.
 * For RDMA, connections are established through {@link com.ibm.disni.RdmaEndpoint RdmaEndpoints}
 * instead of {@link ServerConnection ServerConnections}.
 */
public class RdmaTwoSidedServerConnectionManager implements RdmaConnectionManager<ActiveRdmaCommunicator>{

    private NodeEngine engine;
    private RdmaLogger logger;
    private InetSocketAddress localRdmaAddress;
    private RdmaService rdmaService;
    private RdmaTwoSidedServer server;
    private Collection<CPMember> cpMembers;
    private CPMember localCPMember;
    // Rdma configuration
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> serverEndpointGroup;
    private RdmaTwoSidedEndpointFactory serverEndpointFactory;
    private RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private RdmaConfig rdmaConfig;
    // Rdma rpc
    private Consumer<Packet> packetDispatcher;
    // Actual connection implementations
    private Thread rdmaServerAcceptorTask;
    private FutureTask<Collection<InetSocketAddress>> rdmaServerConnectorTask;
    private RemoteConnector remoteRdmaConnector; // won't run on a separate thread
    // Connected endpoints
    private Map<String, RdmaServerConnection> inboundConnections;
    private Map<String, RdmaServerConnection> outboundConnections;
    private Map<String, RdmaServerConnection> cachedEndpoints;


    public RdmaTwoSidedServerConnectionManager(NodeEngine engine,
                                               Consumer<Packet> packetDispatcher,
                                               RdmaTwoSidedServer server, RdmaConfig rdmaConfig) {
        this.engine = engine;
        this.logger = new RdmaLogger(engine.getLogger(RdmaTwoSidedServerConnectionManager.class));
        this.server = server;
        this.rdmaService = engine.getRdmaService();
        this.rdmaConfig = rdmaConfig;
        this.packetDispatcher = packetDispatcher;
        initializeConnectionDataStructures();
    }

    // Needed for resetting connections
    private void initializeConnectionDataStructures(){
        inboundConnections = new HashMap<>();
        outboundConnections = new HashMap<>();
        cachedEndpoints = new HashMap<>();
    }

    // bind server endpoint to configured address.
    private boolean bindServer(){
        InetSocketAddress localRdmaAddress;
        String rdmaAddressStr = rdmaConfig.getRdmaAddress();
        int port = rdmaConfig.getRdmaListeningPort();
        int serverBacklog = rdmaConfig.getServerBacklog();

        localRdmaAddress = new InetSocketAddress(rdmaAddressStr, port);
        try {
            serverEndpoint.bind(localRdmaAddress, serverBacklog);
            logger.info("Server bound to address : " + localRdmaAddress.toString());
            return true;
        } catch (Exception e) {
            logger.severe("Server failed to bind to provided IP address and available ports.");
            return false;
        }
    }

    @Override
    public boolean initializeRdmaCommunications(Collection<CPMember> cpMembers, CPMember localCPMember){
        this.cpMembers = cpMembers;
        this.localCPMember = localCPMember;
        try {
            // create the server endpoint that will accept rdma connections
            serverEndpointGroup = new RdmaActiveEndpointGroup<>(rdmaConfig.getTimeout(),
                    rdmaConfig.isPolling(), rdmaConfig.getMaxWRs(),
                    rdmaConfig.getMaxSge(), rdmaConfig.getCqSize());
            serverEndpointFactory = new RdmaTwoSidedEndpointFactory(serverEndpointGroup,
                    rdmaConfig.getMaxBufferSize(), rdmaConfig.getMaxWRs(), engine, this);
            serverEndpointGroup.init(serverEndpointFactory);
            serverEndpoint = serverEndpointGroup.createServerEndpoint();

            if(!bindServer()){
                rdmaService.setState(RdmaServiceState.COMMUNICATIONS_NOT_POSSIBLE);
                return false;
            }
            // now we can prepare the tasks that will establish connections
            List<InetSocketAddress> remoteIps = cpMembers.stream()
                .filter(member -> !member.equals(localCPMember)) // avoid connecting to ourselves
                .map(CPMember::getAddress)
                .map(address -> {
                    InetSocketAddress socketAddress = null;
                    try {
                        socketAddress = address.getInetSocketAddress();
                    } catch (UnknownHostException e) {
                        logger.severe(e);
                    }
                    return socketAddress;
                })
                .collect(Collectors.toList());

            rdmaServerAcceptorTask = new Thread(new RdmaServerAcceptor(engine, this,
                    serverEndpoint, inboundConnections));
            rdmaServerConnectorTask = new FutureTask<>(new RemoteConnectorTask(
                    new RetryingConnectorImpl(engine, this,
                            outboundConnections, rdmaConfig),
                    remoteIps));
            remoteRdmaConnector = new SingleTimeConnectorImpl(engine, this,
                    outboundConnections, rdmaConfig);
        } catch (Exception e) {
            logger.severe(e);
            return false;
        }
        return true;
    }

    @Override
    public void startConnecting(){
        rdmaServerAcceptorTask.start();
        try {
            rdmaServerConnectorTask.run();
            Collection<InetSocketAddress> failedConnections = (Collection<InetSocketAddress>)
                    rdmaServerConnectorTask.get();
            if(failedConnections.size() > 0){
                logger.severe("Failed to connect to the following CP members : " +
                        failedConnections.stream().map(Objects::toString)
                                .collect(Collectors.joining(", ")));
            }else{
                logger.info("Finished establishing connections towards all CP members.");
                rdmaService.setState(RdmaServiceState.CONNECTIONS_READY);

            }
        } catch (InterruptedException | ExecutionException e) {
            logger.severe(e);
        } catch (CancellationException e){
            logger.info("Connecting to remote CP members canceled.");
        }
    }

    @Override
    public void stopAndRemoveConnections(){
        if(rdmaServerAcceptorTask != null){
            rdmaServerAcceptorTask.interrupt();
        }
        if(rdmaServerConnectorTask != null){
            rdmaServerConnectorTask.cancel(true);
        }
        outboundConnections.values().forEach(connection -> {
            connection.close("Stopping", null);
        });
        inboundConnections.values().forEach(connection -> {
            connection.close("Stopping", null);
        });
        // reset the connection data structures (clearing them might be slower
        // than creating new objects, when having a lot of connections)
        initializeConnectionDataStructures();
    }

    @Override
    public RdmaServer<ActiveRdmaCommunicator> getServer() {
        return server;
    }

    @Override
    public RdmaServerConnection getRdmaServerConnection(String inetAddressStr){
        /* For each remote side we maintain two endpoints. One is connected by the remote side to our RDMA server
         * and one is connected from our side to the remote RDMA server. To minimize the endpoint state maintained
         * by the network card, we should choose one of these endpoints, cache it and use it for all our communications
         * to the remote side. The other endpoint connecting to that side should be eventually closed. */
        RdmaServerConnection remoteConnection = cachedEndpoints.get(inetAddressStr);
        if(remoteConnection == null){
            remoteConnection = outboundConnections.get(inetAddressStr);
            if(remoteConnection == null){
                remoteConnection =  inboundConnections.get(inetAddressStr);
                if(remoteConnection == null){
                    return null;   // there's no such a connection to the remote side
                }
            }
            cachedEndpoints.put(inetAddressStr, remoteConnection);
        }
        return remoteConnection;
    }

    @Override
    public boolean isConnectedWithRdma(Address address){
        try {
            String inetAddressStr = address.getInetAddress().toString();
            if(cachedEndpoints.get(inetAddressStr) != null){
                return true;
            }else if(inboundConnections.get(inetAddressStr) != null){
                return true;
            }else if(outboundConnections.get(inetAddressStr) != null){
                return true;
            }
        } catch (UnknownHostException e) {
            logger.severe(e);
            return false;
        }
        return false;
    }

    @Override
    public boolean transmit(Packet packet, Address target, int streamId) {
        checkNotNull(packet, "packet can't be null");
        checkNotNull(target, "target can't be null");

        logger.info("Sending packet.");

        try {
            String remoteIp = target.getInetAddress().toString();
            RdmaServerConnection remoteConnection = getRdmaServerConnection(remoteIp);
            // if there's no such a connection, establish one, as the implemented interface suggests
            if(remoteConnection == null){
                boolean connectionSuccess = remoteRdmaConnector.connect(target.getInetSocketAddress());
                if(!connectionSuccess){
                    return false;
                }
                remoteConnection =getRdmaServerConnection(remoteIp);
            }
            return writeToConnection(remoteConnection, packet);
        } catch (UnknownHostException e) {
            logger.severe(e);
        }
        return false;
    }

    /**
     * Does the actual writing of the packet data into the buffer that will be transmitted
     * to the remote side through RDMA.
     * @param remoteConnection  contains the ${@link com.ibm.disni.RdmaEndpoint} that will transfer the data.
     * @param packet the packet data to transmit.
     * @return true on success, false on failure.
     */
    private boolean writeToConnection(RdmaServerConnection remoteConnection, Packet packet){
        ActiveRdmaCommunicator endpoint = remoteConnection.getRdmaEndpoint();
        // Ask for an available Work Request from the Endpoint
        WorkRequestProxy workRequest = endpoint.getWorkRequestProxyProvider()
                .getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
        // Get the buffer associated with the available work request
        ByteBuffer dataBuffer = workRequest.getBuffer();
        // Use a new Packet IO Helper to write to the buffer. We need a new one because it's stateful
        PacketIOHelper packetIOHelper = new PacketIOHelper();
        // write until all data is written
        if(!packetIOHelper.writeTo(packet, dataBuffer)){
            logger.severe("Need larger message buffers. Failed to write full Packet data of size "+packet.dataSize());
            return false;
        }
        dataBuffer.flip();
        // now we can send the data to the remote side
        workRequest.post();
        return true;
    }

    /**
     * Called when a packet is received, to decide what to do with it.
     * @param packet the received packet.
     */
    public void onReceiveFromConnection(Packet packet){
        logger.info("Received packet");
        packetDispatcher.accept(packet);
    }

    /* *************************************************************************
    *   Getters / Setters
     * *************************************************************************/

    public RdmaServerEndpoint<ActiveRdmaCommunicator> getServerEndpoint() {
        return serverEndpoint;
    }

    public void setServerEndpoint(RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint) {
        this.serverEndpoint = serverEndpoint;
    }

    public Consumer<Packet> getPacketDispatcher() {
        return packetDispatcher;
    }
}
