package discovery.client.rpc.response.invocators;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryPacket;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.BooleanSerializer;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.exception.RpcExecutionException;
import jarg.jrcm.rpc.invocation.RpcOperationInvocator;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import jarg.jrcm.rpc.packets.RpcMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class UnregisterServerResponseInvocator implements RpcOperationInvocator {
    private final Logger logger = LoggerFactory.getLogger(UnregisterServerResponseInvocator.class.getSimpleName());

    private BooleanSerializer booleanSerializer;
    private PendingResponseManager responseManager;

    public UnregisterServerResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        booleanSerializer = new BooleanSerializer();
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryPacket rpcPacket = (DiscoveryPacket) packet;
        long operationId = rpcPacket.getOperationId();
        WorkRequestProxy workRequestProxy = rpcPacket.getWorkRequestProxy();

        // check if this is an error response
        if(rpcPacket.getMessageType() == RpcMessageType.ERROR){
            workRequestProxy.releaseWorkRequest();
            // complete future that was waiting for this response
            CompletableFuture<Set<ServerIdentifier>> responseFuture =
                    responseManager.getRegisteredServersPendingResponses().remove(operationId);
            responseFuture.completeExceptionally(
                    new ExecutionException(new RpcExecutionException("Got an ERROR response from the service.")));
        }

        try{
            // deserialize response
            booleanSerializer.setWorkRequestProxy(workRequestProxy);
            booleanSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            Boolean success = booleanSerializer.getFlag();
            // complete future that was waiting for this response
            CompletableFuture<Boolean> responseFuture =
                    responseManager.unregisterServerPendingResponses().remove(operationId);
            responseFuture.complete(success);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response");
            workRequestProxy.releaseWorkRequest();
        }
    }
}
