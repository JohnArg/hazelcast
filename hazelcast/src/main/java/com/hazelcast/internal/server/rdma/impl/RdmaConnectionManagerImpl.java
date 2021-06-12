package com.hazelcast.internal.server.rdma.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.internal.operation.integration.AppendSuccessResponseOp;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.RdmaService;
import com.hazelcast.internal.networking.rdma.RdmaServiceState;
import com.hazelcast.internal.networking.rdma.util.LatencyKeeper;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.benchmarks.timestamps.TimeStampManager;
import com.hazelcast.internal.server.rdma.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.RdmaServer;
import com.hazelcast.internal.server.rdma.RdmaServerConnection;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import discovery.client.DiscoveryClient;
import discovery.client.rpc.DiscoveryServiceProxy;
import discovery.common.api.ServerIdentifier;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcExecutionException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Manages the RDMA connections on behalf of an {@link RdmaServer}.
 */
public class RdmaConnectionManagerImpl implements RdmaConnectionManager<ActiveRdmaCommunicator>{

    private NodeEngine engine;
    private RdmaLogger logger;
    private RdmaService rdmaService;
    private RdmaServerImpl server;
    private Member localMember;
    // Rdma configuration
    private InetSocketAddress localRdmaAddress;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private RdmaTwoSidedCommunicatorFactory serverEndpointFactory;
    private RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private RdmaConfig rdmaConfig;
    // Rdma rpc
    private Consumer<Packet> packetDispatcher;
    // Connected endpoints
    /* Since TCP runs alongside RDMA and NodeEngineRaftIntegration passes us TCP addresses
    * of members when send() is called, we keep a Map of <TCP address, RDMA connection> to
    * retrieve the RDMA connection of a member using its TCP address. When Members of the
    * cluster register to the RDMA discovery service, they register both their TCP and their
    * RDMA address, making it possible to retrieve this information and store it to the following
    * Map.*/
    private ConcurrentMap<InetSocketAddress, RdmaServerConnection> tcpToRdmaMap;
    private ConcurrentMap<InetSocketAddress, RdmaServerConnection> rdmaAddressConnectionMap;
    private ConcurrentMap<InetSocketAddress, ActiveRdmaCommunicator> inboundConnections;
    private Thread memberAcceptorThread;
    private Thread memberConnectorThread;
    // discovery service
    private ServerIdentifier localServerIdentifier;
    private Set<ServerIdentifier> registeredServers;
    private InetSocketAddress discoveryAddress;
    private DiscoveryClient discoveryClient;
    private DiscoveryServiceProxy discoveryAPI;
    // Latency benchmark related
    private TimeStampManager timeStampManager;
    private LatencyKeeper latencyKeeper;


    public RdmaConnectionManagerImpl(NodeEngine engine,
                                     Consumer<Packet> packetDispatcher,
                                     RdmaServerImpl server, RdmaConfig rdmaConfig) {
        this.engine = engine;
        this.localMember = engine.getLocalMember();
        this.logger = new RdmaLogger(engine.getLogger(RdmaConnectionManagerImpl.class.getSimpleName()));
        this.server = server;
        this.rdmaService = engine.getRdmaService();
        this.rdmaConfig = rdmaConfig;
        this.packetDispatcher = packetDispatcher;
        initializeConnectionDataStructures();
    }

    // Needed for resetting connections
    private void initializeConnectionDataStructures(){
        tcpToRdmaMap = new ConcurrentHashMap<>();
        inboundConnections = new ConcurrentHashMap<>();
        rdmaAddressConnectionMap = new ConcurrentHashMap<>();
    }

    // bind server endpoint to configured address.
    private boolean bindServer(){
        String rdmaAddressStr = rdmaConfig.getRdmaAddress();
        int minPort = rdmaConfig.getRdmaListeningPortMin();
        int maxPort = rdmaConfig.getRdmaListeningPortMax();
        int serverBacklog = rdmaConfig.getServerBacklog();

        boolean success = false;
        // try to bind to one of the available ports
        for(int port = minPort; port <= maxPort; port ++){
            localRdmaAddress = new InetSocketAddress(rdmaAddressStr, port);
            try {
                serverEndpoint = endpointGroup.createServerEndpoint();
                serverEndpoint.bind(localRdmaAddress, serverBacklog);
                logger.info("Server bound to address : " + localRdmaAddress.toString());
                success = true;
                break;
            } catch (Exception e) {
                //ignore
            }
        }
        if(!success){
            logger.severe("Server failed to bind to provided IP address and available ports.");
        }
        return success;
    }

    private boolean initializeRdmaCommunications(){
        try {
            // create the server endpoint that will accept rdma connections
            endpointGroup = new RdmaActiveEndpointGroup<>(rdmaConfig.getTimeout(),
                    rdmaConfig.isPolling(), rdmaConfig.getMaxWRs(),
                    rdmaConfig.getMaxSge(), rdmaConfig.getCqSize());
            serverEndpointFactory = new RdmaTwoSidedCommunicatorFactory(
                    (RdmaActiveEndpointGroup<ActiveRdmaCommunicator>) endpointGroup,
                    rdmaConfig.getMaxBufferSize(), rdmaConfig.getMaxWRs(), engine, this);
            endpointGroup.init(serverEndpointFactory);
            // try to bind server to a port
            if(!bindServer()){
                rdmaService.setState(RdmaServiceState.COMMUNICATIONS_NOT_POSSIBLE);
                return false;
            }
            // register to the discovery service and get the members connected so far
            InetAddress discoveryIp = InetAddress.getByName(rdmaConfig.getDiscoveryAddress());
            discoveryAddress = new InetSocketAddress(discoveryIp, rdmaConfig.getDiscoveryPort());
            discoveryClient = new DiscoveryClient(discoveryAddress, rdmaConfig.getTimeout(), rdmaConfig.getMaxWRs(),
                    rdmaConfig.getCqSize(), rdmaConfig.getTimeout(), rdmaConfig.isPolling(), rdmaConfig.getMaxSge(),
                    rdmaConfig.getMaxBufferSize());
            discoveryAPI = discoveryClient.generateDiscoveryServiceProxy();
            if(discoveryAPI == null){
                rdmaService.setState(RdmaServiceState.COMMUNICATIONS_NOT_POSSIBLE);
                serverEndpoint.close();
                logger.severe("Cannot reach discovery service.");
                return false;
            }
            localServerIdentifier = new ServerIdentifier(localRdmaAddress,
                    localMember.getAddress().getInetSocketAddress());
            registeredServers = discoveryAPI.registerServer(localServerIdentifier);
            // no need to keep the connection
            discoveryClient.disconnect();
            if(registeredServers == null){
                logger.severe("Could not get registered servers from remote discovery service.");
                return false;
            }
            StringBuilder messageBuilder = new StringBuilder("Registered to discovery service. Registered servers : ");
            registeredServers.forEach((server)->{messageBuilder.append(server.toString() +" ");});
            logger.info(messageBuilder.toString());
            // prepare threads for connections
            memberAcceptorThread = new Thread(new MemberAcceptor(engine, serverEndpoint, inboundConnections,
                    this));
            registeredServers.remove(localServerIdentifier);
            memberConnectorThread = new Thread(new MemberConnector(endpointGroup, tcpToRdmaMap,
                    rdmaAddressConnectionMap, rdmaConfig, registeredServers, localServerIdentifier, engine,
                    this));
        } catch (Exception e) {
            logger.severe(e);
            return false;
        }
        return true;
    }

    @Override
    public void start(){
        if(!initializeRdmaCommunications()){
            return;
        }
        timeStampManager = ((NodeEngineImpl) engine).getTimeStampManager();
        latencyKeeper = ((NodeEngineImpl) engine).getLatencyKeeper();
        memberAcceptorThread.start();
        memberConnectorThread.start();
        // communications ready
        rdmaService.setState(RdmaServiceState.CONNECTIONS_READY);
    }

    @Override
    public void stopAndRemoveConnections(){
        if((discoveryClient != null) && (localServerIdentifier != null)){
            discoveryAPI = discoveryClient.generateDiscoveryServiceProxy();
            try {
                discoveryAPI.unregisterServer(localServerIdentifier);
            } catch (RpcExecutionException e) {
                logger.severe("Could not unregister server from discovery service.", e);
            }
            discoveryClient.shutDown();
        }
        for(Map.Entry<InetSocketAddress, RdmaServerConnection> connection : rdmaAddressConnectionMap.entrySet()){
            try{
                connection.getValue().getRdmaEndpoint().close();
                logger.info("Closed connection towards "+connection.getKey());
            } catch (InterruptedException | IOException e) {
                // ignore - the remote side might have already disconnected from this server
            }
        }
        try {
            serverEndpoint.close();
        } catch (IOException | InterruptedException e) {
            logger.warning("Error while closing endpoint.", e);
        }

        try{
            endpointGroup.close();
        } catch (IOException | InterruptedException e) {
            logger.warning("Error while closing endpoint group.", e);
        }
        // reset the connection data structures (clearing them might be slower
        // than creating new objects, when having a lot of connections)
        initializeConnectionDataStructures();
    }

    @Override
    public RdmaServer<ActiveRdmaCommunicator> getServer() {
        return server;
    }

    @Override
    public RdmaServerConnection getRdmaServerConnection(InetSocketAddress rdmaAddress){
        return rdmaAddressConnectionMap.get(rdmaAddress);
    }

    @Override
    public boolean isConnectedWithRdma(Address address, boolean isTcpAddress){
        try {
            // if we're given a TCP address
            if(isTcpAddress){
                RdmaServerConnection rdmaServerConnection = tcpToRdmaMap.get(address.getInetSocketAddress());
                return rdmaServerConnection != null;
            }else{// if we're given an RDMA address
                RdmaServerConnection rdmaServerConnection = rdmaAddressConnectionMap
                        .get(address.getInetSocketAddress());
                return rdmaServerConnection != null;
            }
        } catch (UnknownHostException e) {
            logger.severe("Cannot translate Address to InetSocketAddress.", e);
        }
        return false;
    }

    @Override
    public void removeConnection(Address address, boolean isTcpAddress) {
        InetSocketAddress socketAddress = null;
        try {
            socketAddress = address.getInetSocketAddress();
        } catch (UnknownHostException e) {
            return;
        }
        try {
            RdmaServerConnection connectionToRemove = null;
            if(isTcpAddress) {
                connectionToRemove = tcpToRdmaMap.remove(socketAddress);
                if (connectionToRemove == null) {
                    return;
                }
                rdmaAddressConnectionMap.remove(connectionToRemove.getServerIdentifier().getRdmaAddress());
            }else {
                connectionToRemove = rdmaAddressConnectionMap.remove(socketAddress);
                if (connectionToRemove == null) {
                    return;
                }
                tcpToRdmaMap.remove(connectionToRemove.getServerIdentifier().getTcpAddress());
            }
            connectionToRemove.getRdmaEndpoint().close();
            InetSocketAddress rdmaAddress = connectionToRemove.getServerIdentifier().getRdmaAddress();
            inboundConnections.remove(rdmaAddress);
            logger.info("Removed RDMA connection towards " + rdmaAddress);
        } catch (IOException | InterruptedException e) {
            //ignore - if the remote side has shut down correctly,
            // they probably closed their endpoint connecting to us.
        }
    }

    @Override
    public boolean transmit(Packet packet, Address target, int streamId) {
        checkNotNull(packet, "packet can't be null");
        checkNotNull(target, "target can't be null");
        try {
            // find the RDMA connection from the TCP address
            RdmaServerConnection remoteConnection = tcpToRdmaMap.get(target.getInetSocketAddress());
            if(remoteConnection == null){
                return false;
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
    public boolean writeToConnection(RdmaServerConnection remoteConnection, Packet packet){
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
        // the following code was inserted in order to identify which RPC id does the operation
        // have. This helps in associating a timestamp with the RPC.
        // Do not use when benchmarking the network latency of RPCs.
        // Use only for measuring the time between dispatching the packet and executing the operation.
        Object obj = engine.toObject(packet);
        if(obj instanceof AppendSuccessResponseOp){
            latencyKeeper.startLatencies.add(System.nanoTime());
        }
        // end of extra code ======================================================
        packetDispatcher.accept(packet);
    }

    /**
     * Used for SERVER_CONTROL packets that are meant to be directly
     * handled by this connection manager.
     */
    public void handleServerRegistration(ServerIdentifier serverIdentifier, ActiveRdmaCommunicator communicator,
                                         InetSocketAddress senderAddress){
        // Create a Server connection for the remote sender and store it.
        // It will be used during RDMA communications from now on.
        RdmaServerConnection rdmaServerConnection = new RdmaServerConnection(engine, communicator,
                this, new Address(senderAddress), serverIdentifier);
        tcpToRdmaMap.put(serverIdentifier.getTcpAddress(), rdmaServerConnection);
        /* Save the sender address and not the server identifier RDMA address!!!
         The Sender address is the address used to send us a message, since the remote side connected
         to us. We will be responding to the same address. The server identifier RDMA address is the
         address that the remote site is listening to connections. We don't need this, since they
         connected to us already.
         */
        rdmaAddressConnectionMap.put(senderAddress, rdmaServerConnection);
        logger.info("Registered connection for member with identifier : " + serverIdentifier +
                        " and RDMA address : " + senderAddress);
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

    public InetSocketAddress getLocalRdmaAddress() {
        return localRdmaAddress;
    }
}
