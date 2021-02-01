package discovery.client.rpc.response.invocators;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryPacket;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.exception.RpcExecutionException;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class GetRegisteredServersResponseInvocator implements RpcOperationInvocator {

    private final Logger logger = LoggerFactory.getLogger(GetRegisteredServersResponseInvocator.class.getSimpleName());

    private ServerIdentifierSetSerializer serverIdentifierSetSerializer;
    private PendingResponseManager responseManager;

    public GetRegisteredServersResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        serverIdentifierSetSerializer = new ServerIdentifierSetSerializer();
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

        try {
            // deserialize response
            serverIdentifierSetSerializer.setWorkRequestProxy(workRequestProxy);
            serverIdentifierSetSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            Set<ServerIdentifier> members = serverIdentifierSetSerializer.getIdentifiers();
            // complete future that was waiting for this response
            CompletableFuture<Set<ServerIdentifier>> responseFuture =
                    responseManager.getRegisteredServersPendingResponses().remove(operationId);
            responseFuture.complete(members);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response", e);
            workRequestProxy.releaseWorkRequest();
        }
    }
}
