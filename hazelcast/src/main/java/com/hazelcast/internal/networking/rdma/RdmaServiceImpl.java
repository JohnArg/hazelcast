package com.hazelcast.internal.networking.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedServer;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import jarg.rdmarpc.connections.RpcBasicEndpoint;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This service will manage RDMA connections for the Raft protocol,
 * as well as sending and receiving RDMA messages.
 * The actual sending and receiving will be handled by {@link com.ibm.disni.RdmaEndpoint RdmaEndpoints}.
 * The service is meant to be registered in a {@link com.hazelcast.spi.impl.NodeEngine} and listen to
 * membership changes.
 */
public class RdmaServiceImpl implements RdmaService, RaftNodeLifecycleAwareService, RaftManagedService {
    public static final String SERVICE_NAME = "hz:rdma:raft";
    private static final String PROPERTIES_FILE = "rdma.properties";

    private RdmaLogger logger;
    private NodeEngineImpl engine;
    private RaftService raftService;
    private Member localMember;
    private RaftMemberListener memberListener;
    private Collection<CPMember> cpMembers;
    private Thread membershipInfoTask;
    private InternalSerializationService serializationService;
    private Map<RdmaServiceState, List<RdmaServiceListener>> eventListeners;
    // rdma communications ------------------------------------------------
    private RdmaConfig rdmaConfig;
    private RdmaTwoSidedServer rdmaServer;
    private AtomicReference<RdmaServiceState> serviceState;


    public RdmaServiceImpl(NodeEngineImpl engine){
        this.engine = engine;
        this.memberListener = new RaftMemberListener();
        this.membershipInfoTask = new Thread(new MembershipQueryOperation());
        this.rdmaConfig = new RdmaConfig();
        serializationService = (InternalSerializationService) engine.getSerializationService();
        eventListeners = new HashMap<>();
        serviceState = new AtomicReference<>(RdmaServiceState.SERVICE_NOT_READY);
    }

    /* *****************************************************************
     * Managing service
     * ****************************************************************/

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // initialize fields that we couldn't in the constructor
        localMember = engine.getLocalMember();
        logger = new RdmaLogger(engine.getLogger(RdmaServiceImpl.class));

        // Check if system can use RDMA. If not, stop here.
        if(!canUseRDMACheck()){
            logger.info("The server cannot use RDMA. The RDMA service will not be started.");
            return;
        }

        // load RDMA configuration
        try {
            rdmaConfig.loadFromProperties(PROPERTIES_FILE);
        } catch (Exception e) {
            logger.severe("Could not load RDMA settings from properties file. " +
                    e.getMessage() + ". Setting defaults.");
            rdmaConfig.setDefaults();
        }
        // instantiate an RDMA server but don't run it yet
        rdmaServer = new RdmaTwoSidedServer(engine, engine.getPacketDispatcher(), serializationService, rdmaConfig);
        // Register a listener for membership events fired in the CP subsystem
        // We need this to know when to create or destroy RDMA Endpoints
        raftService = (RaftService) engine.getService(RaftService.SERVICE_NAME);
        raftService.registerMembershipListener(memberListener);
        logger.info("RdmaService initialized.");
        // find all active members so far and then start an RDMA server and initialize
        // RDMA client endpoints, only between those that are CP members.
        membershipInfoTask.start();
    }

    private boolean canUseRDMACheck(){

        /* Todo - Check for RDMA capable NIC */

        /* If the local member IP address is localhost or 127.0.0.1
        RDMA cannot bind and listen to it. */
        try {
            String localAddress= localMember.getAddress().getInetAddress().getHostAddress();
            if(localAddress.equals("127.0.0.1")){
                logger.info("RDMA cannot bind to 127.0.0.1 or localhost.");
                return false;
            }
        } catch (UnknownHostException e) {
            logger.severe(e);
            return false;
        }

        return true;
    }

    @Override
    public void onCPSubsystemRestart() {
        reset();
    }


    @Override
    public void reset() {
        logger.info("Reset called");
        setState(RdmaServiceState.SERVICE_NOT_READY);
        // terminate the server
        if(rdmaServer != null){
            rdmaServer.shutdown();
            membershipInfoTask.start();
        }else{
            init(engine,null);
        }
        // the state of the service will be changed once
        // RDMA connections are established by the connection
        // manager. That's why it's not done here.
    }

    @Override
    public void shutdown(boolean terminate) {
        logger.info("Shutting down service.");
        // change state and tell everyone that uses this service that you'll shut down
        setState(RdmaServiceState.SERVICE_SHUTDOWN);
        // terminate the server
        if(rdmaServer != null){
            rdmaServer.shutdown();
        }
    }

    @Override
    public boolean send(Operation op, Address target) {
        return rdmaServer.send(op, target);
    }

    /* *****************************************************************
     * Manage the service's state
     * ****************************************************************/

    @Override
    public RdmaServiceState getLatestState() {
        return serviceState.get();
    }

    @Override
    public void setState(RdmaServiceState newState){
        serviceState.set(newState);
    }

    /* *****************************************************************
     * Starting components if this is a CP member
     * ****************************************************************/

    /**
     * Waits until member discovery is completed before starting an RDMA
     * server and the initialization of RDMA client endpoints.
     */
    private class MembershipQueryOperation implements Runnable{

        @Override
        public void run() {
            // wait until the discovery process is complete
            try {
                raftService.awaitUntilDiscoveryCompleted(rdmaConfig.getDiscoveryTimeout(),
                        TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                logger.severe(e);
            }
            // ask which are the CP members and store the result
            InternalCompletableFuture<Collection<CPMember>> membersFuture = raftService.getCPMembers();
            boolean success = false;
            // make a few attempts in case exceptions are thrown because members are not ready
            for(int i=0; i < rdmaConfig.getCpDiscoveryRetries(); i++){
                try {
                    cpMembers = membersFuture.get();
                    logger.info("Got CP members");
                    success = true;
                    break;
                } catch (Exception e) {
                   //logger.severe(e);
                }
            }
            if(!success){
                logger.severe("Could not get the CP members. The RdmaService will be shutdown.");
                shutdown(true);
            }else{
                CPMember localCPMember = raftService.getLocalCPMember();
                if( localCPMember != null){
                    // start server
                    logger.info("This is a CP member. Starting the RDMA server.");
                    rdmaServer.setCpMembers(cpMembers, localCPMember);
                    rdmaServer.start();
                }else{
                    logger.info("This is not a CP member, no need to start RDMA communications.");
                }
            }
        }
    }

    @Override
    public boolean isConnectedWithRdma(Address address){
        if(rdmaServer != null){
            RdmaConnectionManager<RpcBasicEndpoint> connectionManager = rdmaServer.getConnectionManager();
            if(connectionManager != null){
                return connectionManager.isConnectedWithRdma(address);
            }
        }
        return false;
    }

    /* *****************************************************************
     *  Listening for events
     * ****************************************************************/

    /**
     * Created to listen for membership changes in the CP subsystem and
     * create or destroy RDMA endpoints accordingly.
     * A new member will require a new RDMA endpoint for communications,
     * while a removed one will have no further need for its RDMA endpoint.
     */
    public class RaftMemberListener implements CPMembershipListener{

        @Override
        public void memberAdded(CPMembershipEvent event) {
            logger.info("Member added : "+ event.getMember().getAddress().getHost());
        }

        @Override
        public void memberRemoved(CPMembershipEvent event) {
            logger.info("Member added : "+ event.getMember().getAddress().getHost());
        }
    }

    @Override
    public void onRaftNodeTerminated(CPGroupId groupId) {
        logger.info("Node "+ localMember.getSocketAddress().getHostString()
                +" with group id "+groupId+ " is terminated.");
    }

    @Override
    public void onRaftNodeSteppedDown(CPGroupId groupId) {
        logger.info("Node "+ localMember.getSocketAddress().getHostString()
                +" with group id "+groupId+ " is stepped down.");
    }

}
