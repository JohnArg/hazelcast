package discovery.service.rpc.invocators;

import discovery.common.DiscoveryPacket;
import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import jarg.rdmarpc.networking.communicators.RdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.exception.RpcExecutionException;
import jarg.rdmarpc.rpc.invocation.AbstractThreadPoolInvocator;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Reads information from an {@link DiscoveryPacket}, deserializes parameters and invokes
 * {@link DiscoveryApi#registerServer(ServerIdentifier)}. It then sends back a response to
 * the caller.
 */
public class RegisterServerApiInvocator extends AbstractDiscoveryInvocator {
    private static final Logger logger = LoggerFactory.getLogger(RegisterServerApiInvocator.class.getSimpleName());

    private DiscoveryApi serviceApi;

    public RegisterServerApiInvocator(DiscoveryApi serviceApi) {
        super(logger);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryPacket requestPacket = (DiscoveryPacket) packet;
        // Pass the packet's work request data to the serializer
        WorkRequestProxy workRequestProxy = requestPacket.getWorkRequestProxy();
        ServerIdentifier identifier = new ServerIdentifier();
        identifier.setWorkRequestProxy(workRequestProxy);

        try {
            // deserialize request parameters from the received packet
            identifier.readFromWorkRequestBuffer();
            // Free WR id, we have the objects we need
            workRequestProxy.releaseWorkRequest();
        } catch (RpcDataSerializationException e) {
            // Free WR id
            workRequestProxy.releaseWorkRequest();
            // Send ERROR Response
            logger.error("Unable to deserialize request parameters.", e);
            sendResponse(requestPacket, null, true);
            return;
        }
        // invoke the service's API
        Set<ServerIdentifier> newMembers = null;
        try {
            newMembers = serviceApi.registerServer(identifier);
        } catch (RpcExecutionException e) {
            // Free WR id
            workRequestProxy.releaseWorkRequest();
            // Send ERROR Response
            logger.error("Error during API execution.", e);
            sendResponse(requestPacket, null, true);
            return;
        }
        // pass the response to the serializer
        ServerIdentifierSetSerializer responseSerializer = new ServerIdentifierSetSerializer();
        responseSerializer.setIdentifiers(newMembers);
        // send the response
        sendResponse(requestPacket, responseSerializer, false);
    }


}
