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
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedServer;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
    private final InternalSerializationService serializationService;
    private Map<RdmaServiceState, List<RdmaServiceListener>> eventListeners;
    // rdma communications ------------------------------------------------
    private RdmaConfig rdmaConfig;
    private RdmaTwoSidedServer rdmaServer;
    private RdmaServiceState serviceState;


    public RdmaServiceImpl(NodeEngineImpl engine){
        this.engine = engine;
        this.memberListener = new RaftMemberListener();
        this.rdmaConfig = new RdmaConfig();
        this.membershipInfoTask = new Thread(new MembershipQueryOperation());
        serializationService = (InternalSerializationService) engine.getSerializationService();
        eventListeners = new HashMap<>();
        serviceState = RdmaServiceState.SERVICE_NOT_READY;
    }

    /* *****************************************************************
     * Managing service
     * ****************************************************************/

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // initialize fields that we couldn't in the constructor
        localMember = engine.getLocalMember();
        logger = new RdmaLogger(engine.getLogger(RdmaServiceImpl.class));
        try {
            rdmaConfig.loadFromProperties(PROPERTIES_FILE);
        } catch (Exception e) {
            logger.severe("Could not load RDMA settings from properties file. " +
                    e.getMessage() + ". Setting defaults.");
            rdmaConfig.setDefaults();
        }
        rdmaServer = new RdmaTwoSidedServer(engine, engine.getPacketDispatcher(), serializationService, rdmaConfig);
        // Register a listener for membership events fired in the CP subsystem
        // We need this to know when to create or destroy RDMA Endpoints
        raftService = (RaftService) engine.getService(RaftService.SERVICE_NAME);
        raftService.registerMembershipListener(memberListener);
        logger.info("RaftRdmaService initialized.");
        // find all active members so far and then start an RDMA server and initialize
        // RDMA client endpoints, only between those that are CP members.
        membershipInfoTask.start();
    }

    @Override
    public void onCPSubsystemRestart() {
        reset();
    }


    @Override
    public void reset() {
        // Todo
        setState(RdmaServiceState.SERVICE_NOT_READY);
        logger.info("Reset called");
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
     * Manage listeners to this service's events (Observer Pattern)
     * ****************************************************************/

    @Override
    public boolean registerListener(RdmaServiceState eventType, RdmaServiceListener listener) {
        List<RdmaServiceListener> listeners = eventListeners.get(eventType);
        if(listeners == null){
            listeners = new ArrayList<>();
            eventListeners.put(eventType, listeners);
            return listeners.add(listener);
        }
        return eventListeners.get(eventType).add(listener);
    }

    @Override
    public boolean removeListener(RdmaServiceState eventType, RdmaServiceListener listener){
        List<RdmaServiceListener> listeners = eventListeners.get(eventType);
        if(listeners == null){
            return false;
        }
        return eventListeners.get(eventType).remove(listener);
    }

    @Override
    public void notifyListeners(RdmaServiceState eventType) {
        List<RdmaServiceListener> listeners = eventListeners.get(eventType);
        if(listeners == null){
            return;
        }
        eventListeners.get(eventType).forEach(listener -> listener.update(eventType));
    }

    /* *****************************************************************
     * Manage the service's state
     * ****************************************************************/

    @Override
    public RdmaServiceState getLatestState() {
        return serviceState;
    }

    @Override
    public void setState(RdmaServiceState newState){
        serviceState = newState;
        notifyListeners(newState);
    }

    /* *****************************************************************
     * Starting components if this is a CP member
     * ****************************************************************/

    /**
     * Waits until member discovery is completed before starting an RDMA
     * server and the initialization of RDMA client endpoints.
     */
    private class MembershipQueryOperation implements Runnable{
        private final int waitDiscoveryTimeout = 500;

        @Override
        public void run() {
            // wait until the discovery process is complete
            try {
                raftService.awaitUntilDiscoveryCompleted(waitDiscoveryTimeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.severe(e);
            }
            // ask which are the CP members and store the result
            InternalCompletableFuture<Collection<CPMember>> membersFuture = raftService.getCPMembers();
            try {
                cpMembers = membersFuture.get();
                logger.info("Got CP members");
            } catch (InterruptedException | ExecutionException e) {
                logger.severe(e);
            }
            CPMember localCPMember = raftService.getLocalCPMember();
            if( localCPMember != null){
                // start server
                logger.info("Starting the RDMA server.");
                rdmaServer.setCpMembers(cpMembers, localCPMember);
                rdmaServer.start();
            }else{
                logger.info("Not a CP member, no need to start RDMA communications.");
            }
        }
    }

    @Override
    public boolean isConnectedWithRdma(Address address){
        return rdmaServer.getConnectionManager().isConnectedWithRdma(address);
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
