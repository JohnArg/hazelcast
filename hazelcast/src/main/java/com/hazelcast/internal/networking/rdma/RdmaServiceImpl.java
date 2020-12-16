package com.hazelcast.internal.networking.rdma;

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.event.CPMembershipEvent;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedServer;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This service will manage RDMA connections for the Raft protocol,
 * as well as sending and receiving RDMA messages.
 * The actual sending and receiving will be handled by {@link com.ibm.disni.RdmaEndpoint RdmaEndpoints}.
 * The service is meant to be registered in a {@link com.hazelcast.spi.impl.NodeEngine} and listen to
 * membership changes.
 */
public class RdmaServiceImpl implements RaftNodeLifecycleAwareService, RaftManagedService {
    public static final String SERVICE_NAME = "hz:rdma:raft";

    private RdmaLogger logger;
    private NodeEngine engine;
    private RaftService raftService;
    private Member localMember;
    private RaftMemberListener memberListener;
    private Collection<CPMember> cpMembers;
    private Thread membershipInfoTask;
    // rdma communications ------------------------------------------------
    private RdmaEndpointSettings rdmaEndpointSettings;
    private RdmaTwoSidedServer rdmaServer;

    private final int RDMA_PORT = 3000;
    private final int serverBacklog = 100;
    private final int connectionRetries = 10;
    private final int connectionRetryDelay = 500;
    private final int timeout = 1000;
    private final boolean polling = false;
    private final int maxWRs = 10;
    private final int cqSize = 10;
    private final int maxSge = 1;
    private final int maxBufferSize = 200;


    public RdmaServiceImpl(NodeEngine engine){
        this.engine = engine;
        this.memberListener = new RaftMemberListener();
        this.rdmaEndpointSettings = new RdmaEndpointSettings(RDMA_PORT, connectionRetries, connectionRetryDelay,
                timeout, polling, maxWRs, maxSge, cqSize, serverBacklog, maxBufferSize);
        this.membershipInfoTask = new Thread(new MembershipQueryOperation());
        this.rdmaServer = null; // will be initialized later
    }

    /* *****************************************************************
     * Managing service
     * ****************************************************************/
    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        // initialize fields that we couldn't in the constructor
        localMember = engine.getLocalMember();
        logger = new RdmaLogger(engine.getLogger(RdmaServiceImpl.class));
        rdmaServer = new RdmaTwoSidedServer(engine, rdmaEndpointSettings);
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
        logger.info("Reset called");
    }

    @Override
    public void shutdown(boolean terminate) {
        // Todo
        logger.info("Shutdown called");
        if(rdmaServer != null){
            rdmaServer.shutdown();
        }
    }


    /* *****************************************************************
     * Managing server/client endpoints
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
