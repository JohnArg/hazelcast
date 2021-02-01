package com.hazelcast.internal.networking.rdma.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftNodeLifecycleAwareService;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.spi.RaftManagedService;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.RdmaService;
import com.hazelcast.internal.networking.rdma.RdmaServiceState;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.rdma.RdmaConnectionManager;
import com.hazelcast.internal.server.rdma.impl.RdmaServerImpl;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.networking.rdma.RdmaServiceState.*;

/**
 * This service will manage RDMA connections for the Raft protocol,
 * as well as sending and receiving RDMA messages.
 * The actual sending and receiving will be handled by {@link com.ibm.disni.RdmaEndpoint RdmaEndpoints}.
 * The service is meant to be registered in a {@link com.hazelcast.spi.impl.NodeEngine} and listen to
 * membership changes.
 */
public class RdmaServiceImpl implements RdmaService, RaftManagedService, MembershipAwareService {
    public static final String SERVICE_NAME = "hz:rdma:raft";
    private static final String RDMA_PROPERTIES_FILE = "rdma.properties";

    private RdmaLogger logger;
    private NodeEngineImpl engine;
    private RaftService raftService;
    private Member localMember;
    private RdmaServiceEventSubscriberImpl eventSubscriber;
    private InternalSerializationService serializationService;
    // rdma communications ------------------------------------------------
    private RdmaConfig rdmaConfig;
    private RdmaServerImpl rdmaServer;
    private AtomicReference<RdmaServiceState> serviceState;


    public RdmaServiceImpl(NodeEngineImpl engine){
        this.engine = engine;
        serializationService = (InternalSerializationService) engine.getSerializationService();
        serviceState = new AtomicReference<>(SERVICE_NOT_READY);
    }

    /* *****************************************************************
     * Service Lifecycle
     * ****************************************************************/

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        logger = new RdmaLogger(engine.getLogger(RdmaServiceImpl.class.getSimpleName()));
        rdmaConfig = new RdmaConfig();
        // Check if system can use RDMA. If not, stop here.
        try{
            rdmaConfig.loadFromProperties(RDMA_PROPERTIES_FILE);
        } catch (Exception e) {
            logger.info("Cannot read RDMA properties from configuration file. RDMA will be " +
                    "disabled.", e);
            rdmaConfig.setRdmaEnable(false);
        }
        // RDMA can also be disabled by setting a property in the configuration file
        if(!rdmaConfig.isRdmaEnabled()){
            logger.info("RDMA is not enabled. The RdmaService will not be started.");
            return;
        }
        // Register a listener for membership events fired in the CP subsystem
        // We need this to know when to create or destroy RDMA Endpoints
        eventSubscriber = new RdmaServiceEventSubscriberImpl(this, engine, localMember);
        raftService = (RaftService) engine.getService(RaftService.SERVICE_NAME);
        raftService.registerMembershipListener(eventSubscriber);
        logger.info("RdmaService initialized.");
        // Start an RDMA server
        localMember = engine.getLocalMember();
        rdmaServer = new RdmaServerImpl(engine, localMember, engine.getPacketDispatcher(),
                serializationService, rdmaConfig);
        rdmaServer.start();
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
        }else{
            init(engine,null);
        }
        // the state of the service will be changed once
        // RDMA connections are established by the connection
        // manager. That's why it's not done here.
    }

    @Override
    public void shutdown(boolean terminate) {
        if(logger != null) { // needed in case of immediate shutdown
            logger.info("Shutting down service.");
        }
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
    public void memberAdded(MembershipServiceEvent event) {
        // ignore, they'll connect to us with RDMA
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        // we don't need to keep the connection
        if(rdmaConfig.isRdmaEnabled()){
            Address memberAddress = event.getMember().getAddress();
            rdmaServer.getConnectionManager().removeConnection(memberAddress, true);
        }
    }

    /* *****************************************************************
     * Using RDMA
     * ****************************************************************/

    @Override
    public boolean isConnectedWithRdma(Address address, boolean isTcpAddress){
        if(rdmaServer != null){
            RdmaConnectionManager<ActiveRdmaCommunicator> connectionManager = rdmaServer.getConnectionManager();
            if(connectionManager != null){
                return connectionManager.isConnectedWithRdma(address, isTcpAddress);
            }
        }
        return false;
    }

    @Override
    public boolean send(Operation op, Address target) {
        return rdmaServer.send(op, target);
    }

    public RdmaConfig getRdmaConfig() {
        return rdmaConfig;
    }
}
