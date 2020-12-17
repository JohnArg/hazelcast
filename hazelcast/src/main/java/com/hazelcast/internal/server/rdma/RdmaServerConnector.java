package com.hazelcast.internal.server.rdma;

import com.hazelcast.cluster.Member;
import com.hazelcast.cp.CPMember;
import com.hazelcast.internal.networking.rdma.RdmaConfig;
import com.hazelcast.internal.networking.rdma.util.RdmaLogger;
import com.hazelcast.internal.server.rdma.twosided.RdmaTwoSidedEndpointFactory;
import com.hazelcast.spi.impl.NodeEngine;
import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.connections.RpcBasicEndpoint;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Attempts to connect to the provided remote members.
 */
public class RdmaServerConnector implements Callable<Collection<CPMember>> {
    private RdmaLogger logger;
    private RdmaActiveEndpointGroup<RpcBasicEndpoint> clientEndpointGroup;
    private RdmaTwoSidedEndpointFactory clientEndpointFactory;
    private Map<String, RpcBasicEndpoint> outboundConnections;
    private RdmaConfig settings;
    private Collection<CPMember> remoteMembers;
    private Collection<CPMember> successfullConnections;
    private Member localMember;


    public RdmaServerConnector(NodeEngine engine, Collection<CPMember> remoteMembers,
                               Map<String, RpcBasicEndpoint> outboundConnections,
                               RdmaConfig settings){
        logger = new RdmaLogger(engine.getLogger(RdmaServerConnector.class));
        this.localMember = engine.getLocalMember();
        this.remoteMembers = remoteMembers;
        this.outboundConnections = outboundConnections;
        this.settings = settings;
        successfullConnections = new ArrayList<>();
        try {
            // prepare a client endpoint group for connecting to other members
            clientEndpointGroup = new RdmaActiveEndpointGroup<>(settings.getTimeout(), settings.isPolling(),
                    settings.getMaxWRs(), settings.getMaxSge(), settings.getCqSize());
            clientEndpointFactory = new RdmaTwoSidedEndpointFactory(clientEndpointGroup,
                    settings.getMaxBufferSize(), settings.getMaxWRs());
            clientEndpointGroup.init(clientEndpointFactory);
        } catch (Exception e) {
            logger.severe(e);
        }
    }

    @Override
    public Collection<CPMember> call() {

        label_connecting : for (CPMember remoteMember : remoteMembers) {
            // If the thread was interrupted, stop processing. This makes this operation possible to cancel.
            if(Thread.interrupted()){
                break;
            }
            // Don't need to do something if it's the local node
            if(remoteMember.getUuid().equals(localMember.getUuid())){
                continue;
            }
            try {
                // RDMA must connect to the same public remote ip as TCP, but in another port
                InetSocketAddress tcpInetAddress = remoteMember.getAddress().getInetSocketAddress();
                InetAddress remoteIp = tcpInetAddress.getAddress();
                InetSocketAddress rdmaAddress = new InetSocketAddress(remoteIp, settings.getRdmaListeningPort());
                String rdmaAddressStr = rdmaAddress.getAddress().toString();
                // Try to connect up to certain number of retries, unless the thread is interrupted
                RpcBasicEndpoint remoteEndpoint;
                boolean success = false;
                int retryDelay = settings.getConnectionRetryDelay();

                for(int retries = settings.getConnectionRetries(); retries > 0; retries --){
                    if(Thread.interrupted()){
                        break label_connecting;
                    }
                    remoteEndpoint = clientEndpointGroup.createEndpoint();
                    try{
                        remoteEndpoint.connect(rdmaAddress, settings.getTimeout());
                        outboundConnections.put(rdmaAddressStr, remoteEndpoint);
                        success = true;
                        logger.info("Connected to server with address "+rdmaAddress);
                        break;
                    }catch (Exception e){
                        //logger.severe(e);
                        remoteEndpoint.close();
                        Thread.sleep(retryDelay);
                    }
                }
                if(!success){
                    logger.severe("Failed to connect to server with IP "+rdmaAddressStr + " " +
                            "after "+ settings.getConnectionRetries() + " retries");
                }else{
                    successfullConnections.add(remoteMember);
                }
            } catch (Exception e) {
                logger.severe(e);
            }
        }
        return successfullConnections;
    }
}
