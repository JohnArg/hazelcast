package com.hazelcast.internal.networking.rdma.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.RdmaService;
import com.hazelcast.internal.networking.rdma.RdmaServiceState;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedServer;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.networking.rdma.RdmaServiceState.*;

/**
 * This service will manage RDMA connections for the Raft protocol,
 * as well as sending and receiving RDMA messages.
 * The actual sending and receiving will be handled by {@link com.ibm.disni.RdmaEndpoint RdmaEndpoints}.
 * The service is meant to be registered in a {@link com.hazelcast.spi.impl.NodeEngine} and listen to
 * membership changes.
 */
public class RdmaServiceImpl implements RdmaService, RaftManagedService, RaftNodeLifecycleAwareService{
    public static final String SERVICE_NAME = "hz:rdma:raft";
    private static final String PROPERTIES_FILE = "rdma.properties";

    private RdmaLogger logger;
    private NodeEngineImpl engine;
    private RaftService raftService;
    private Member localMember;
    private RdmaServiceEventSubscriberImpl eventSubscriber;
    private Collection<CPMember> cpMembers;
    private Thread membershipInfoTask;
    private InternalSerializationService serializationService;
    // rdma communications ------------------------------------------------
    private RdmaConfig rdmaConfig;
    private RdmaTwoSidedServer rdmaServer;
    private AtomicReference<RdmaServiceState> serviceState;


    public RdmaServiceImpl(NodeEngineImpl engine){
        this.engine = engine;
        this.membershipInfoTask = new Thread(new MembershipQueryOperation());
        this.rdmaConfig = new RdmaConfig();
        serializationService = (InternalSerializationService) engine.getSerializationService();
        serviceState = new AtomicReference<>(SERVICE_NOT_READY);
    }

    /* *****************************************************************
     * Service Lifecycle
     * ****************************************************************/

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = new RdmaLogger(engine.getLogger(RdmaServiceImpl.class.getSimpleName()));
        // Check if system can use RDMA. If not, stop here.
        if(!canUseRDMA()){
            logger.info("The server cannot use RDMA. The RDMA service will not be started.");
            return;
        }
        // initialize fields that we couldn't in the constructor
        localMember = engine.getLocalMember();
        eventSubscriber = new RdmaServiceEventSubscriberImpl(this, engine, localMember);
        // instantiate an RDMA server but don't run it yet
        rdmaServer = new RdmaTwoSidedServer(engine, engine.getPacketDispatcher(), serializationService, rdmaConfig);
        // Register a listener for membership events fired in the CP subsystem
        // We need this to know when to create or destroy RDMA Endpoints
        raftService = (RaftService) engine.getService(RaftService.SERVICE_NAME);
        raftService.registerMembershipListener(eventSubscriber);
        logger.info("RdmaService initialized.");
        // find all active members so far and then start an RDMA server and initialize
        // RDMA client endpoints, only between those that are CP members.
        membershipInfoTask.start();
    }

    /**
     * Verifies that RDMA can be used.
     * @return
     */
    private boolean canUseRDMA(){
        /* Todo - Check for RDMA capable NIC */
        // load RDMA configuration
        try {
            rdmaConfig.loadFromProperties(PROPERTIES_FILE);
        } catch (Exception e) {
            logger.severe("Could not load RDMA settings from properties file. RDMA service will not proceed.",
                    e);
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
        setState(SERVICE_NOT_READY);
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
        setState(SERVICE_SHUTDOWN);
        // terminate the server
        if(rdmaServer != null){
            rdmaServer.shutdown();
        }
    }

    @Override
    public RdmaServiceState getLatestState() {
        return serviceState.get();
    }

    @Override
    public void setState(RdmaServiceState newState){
        serviceState.set(newState);
        if(newState.equals(COMMUNICATIONS_NOT_POSSIBLE)){
            logger.info("Cannot establish RDMA communications. Service is being shut down.");
            shutdown(true);
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

    /* *****************************************************************
     * Starting/Using RDMA components if this is a CP member
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
                logger.severe("Could not get the CP members. The service will not be used.");
            }else{
                CPMember localCPMember = raftService.getLocalCPMember();
                if( localCPMember != null){
                    boolean readyToStart = rdmaServer.canConnectToCpMembersWithRdma(cpMembers, localCPMember);
                    if(readyToStart) {
                        logger.info("This is a CP member. Starting the RDMA server.");
                        rdmaServer.start();
                    }else {
                        logger.info("Not possible to establish RDMA connections. TCP will be used instead " +
                                "of RDMA for CP member communications.");
                    }
                }else{
                    logger.info("This is not a CP member, no need to start RDMA communications.");
                }
            }
        }
    }

    @Override
    public boolean isConnectedWithRdma(Address address){
        if(rdmaServer != null){
            RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager = rdmaServer.getConnectionManager();
            if(connectionManager != null){
                return connectionManager.isConnectedWithRdma(address);
            }
        }
        return false;
    }

    @Override
    public boolean send(Operation op, Address target) {
        return rdmaServer.send(op, target);
    }

}
