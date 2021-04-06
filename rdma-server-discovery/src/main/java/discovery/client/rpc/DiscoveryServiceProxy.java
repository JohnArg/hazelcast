package discovery.client.rpc;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.DiscoveryRpcPacketFactory;
import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import jarg.jrcm.networking.communicators.RdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.exception.RpcExecutionException;
import jarg.jrcm.rpc.packets.RpcMessageType;
import jarg.jrcm.rpc.request.RequestIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Acts the client stub for discovery RPCs.
 */
public class DiscoveryServiceProxy implements DiscoveryApi {

    private Logger logger = LoggerFactory.getLogger(DiscoveryServiceProxy.class);

    private RdmaCommunicator rdmaCommunicator;
    private DiscoveryRpcPacketFactory packetFactory;
    private PendingResponseManager pendingResponseManager;
    private int rpcTimeout;

    public DiscoveryServiceProxy(RdmaCommunicator rdmaCommunicator, PendingResponseManager pendingResponseManager,
                                 RequestIdGenerator<Long> requestIdGenerator, int rpcTimeout) {
        this.rdmaCommunicator = rdmaCommunicator;
        this.pendingResponseManager = pendingResponseManager;
        this.packetFactory = new DiscoveryRpcPacketFactory(requestIdGenerator);
        this.rpcTimeout = rpcTimeout;
    }

    @Override
    public Set<ServerIdentifier> registerServer(ServerIdentifier identifier) throws RpcExecutionException {
        DiscoveryPacket requestPacket = generateRequestPacket(DiscoveryOperationType.REGISTER_SERVER);
        // Send request parameters to serializer
        identifier.setWorkRequestProxy(requestPacket.getWorkRequestProxy());
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(identifier);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            throw new RpcExecutionException("Cannot serialize RPC packet.", e);
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getOperationId();
        CompletableFuture<Set<ServerIdentifier>> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.registerServerPendingResponses().put(operationId, pendingResponse);
        Set<ServerIdentifier> members = null;
        // Send the Work Request to the NIC
        requestPacket.getWorkRequestProxy().post();
        // Wait for response
        try {
            members = pendingResponse.get(rpcTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RpcExecutionException("Failed to execute RPC", e);
        }
        return members;
    }

    @Override
    public boolean unregisterServer(ServerIdentifier identifier) throws RpcExecutionException{
        DiscoveryPacket requestPacket = generateRequestPacket(DiscoveryOperationType.UNREGISTER_SERVER);
        // Send request parameters to serializer
        identifier.setWorkRequestProxy(requestPacket.getWorkRequestProxy());
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(identifier);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            throw new RpcExecutionException("Cannot serialize RPC packet.", e);
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getOperationId();
        CompletableFuture<Boolean> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.unregisterServerPendingResponses().put(operationId, pendingResponse);
        boolean success = false;
        // Send the Work Request to the NIC
        requestPacket.getWorkRequestProxy().post();
        // Wait for response
        try {
            success = pendingResponse.get(rpcTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RpcExecutionException("Failed to execute RPC", e);
        }
        return success;
    }

    @Override
    public Set<ServerIdentifier> getRegisteredServers() throws RpcExecutionException{
        DiscoveryPacket requestPacket = generateRequestPacket(DiscoveryOperationType.GET_SERVERS);
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(null);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            throw new RpcExecutionException("Cannot serialize RPC packet.", e);
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getOperationId();
        CompletableFuture<Set<ServerIdentifier>> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.getRegisteredServersPendingResponses().put(operationId, pendingResponse);
        Set<ServerIdentifier> members = null;
        // Send the Work Request to the NIC
        requestPacket.getWorkRequestProxy().post();
        // Wait for response
        try {
            members = pendingResponse.get(rpcTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Failed to get response.", e);
            throw new RpcExecutionException("Failed to execute RPC", e);
        }
        return members;
    }

    /**
     * Helper function that generates a request RPC packet to send to the remote side.
     */
    private DiscoveryPacket generateRequestPacket(int operationType){
        // Get an available Work Request from the communicator
        WorkRequestProxy workRequestProxy = rdmaCommunicator.getWorkRequestProxyProvider()
                .getPostSendRequestBlocking(WorkRequestType.TWO_SIDED_SEND_SIGNALED);
        // Generate a request packet
        return packetFactory.generatePacket(workRequestProxy, RpcMessageType.REQUEST, operationType);
    }
}
