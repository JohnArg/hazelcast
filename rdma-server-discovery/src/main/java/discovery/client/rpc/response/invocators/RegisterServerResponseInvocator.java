package discovery.client.rpc.response.invocators;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryPacket;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;


public class RegisterServerResponseInvocator implements RpcOperationInvocator {
    private final Logger logger = LoggerFactory.getLogger(RegisterServerResponseInvocator.class.getSimpleName());

    private ServerIdentifierSetSerializer serverIdentifierSetSerializer;
    private PendingResponseManager responseManager;

    public RegisterServerResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        this.serverIdentifierSetSerializer = new ServerIdentifierSetSerializer();
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryPacket rpcPacket = (DiscoveryPacket) packet;
        long operationId = rpcPacket.getOperationId();
        WorkRequestProxy workRequestProxy = rpcPacket.getWorkRequestProxy();

        try {
            // deserialize response
            serverIdentifierSetSerializer.setWorkRequestProxy(workRequestProxy);
            serverIdentifierSetSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            Set<ServerIdentifier> previousServers = serverIdentifierSetSerializer.getIdentifiers();
            // complete future that was waiting for this response
            CompletableFuture<Set<ServerIdentifier>> responseFuture =
                    responseManager.registerServerPendingResponses().remove(operationId);
            responseFuture.complete(previousServers);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response");
            workRequestProxy.releaseWorkRequest();
        }
    }
}
