package discovery.service.rpc.invocators;

import discovery.common.DiscoveryPacket;
import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcExecutionException;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Reads information from an {@link DiscoveryPacket}, deserializes parameters and invokes
 * {@link DiscoveryApi#getRegisteredServers()}. It then sends back a response to
 * the caller.
 */
public class GetRegisteredServersApiInvocator extends AbstractDiscoveryInvocator{

    private static final Logger logger = LoggerFactory.getLogger(GetRegisteredServersApiInvocator.class.getSimpleName());

    private DiscoveryApi serviceApi;

    public GetRegisteredServersApiInvocator(DiscoveryApi serviceApi) {
        super(logger);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryPacket requestPacket = (DiscoveryPacket) packet;
        // Get the packet's work request data
        WorkRequestProxy workRequestProxy = requestPacket.getWorkRequestProxy();
        // Free WR id, we have the objects we need
        workRequestProxy.releaseWorkRequest();
        // invoke the service's API
        Set<ServerIdentifier> members = null;
        try {
            members = serviceApi.getRegisteredServers();
        } catch (RpcExecutionException e) {
            // Free WR id
            workRequestProxy.releaseWorkRequest();
            // Send ERROR Response
            logger.error("Error during API execution.", e);
            sendResponse(requestPacket, null, true);
            return;
        }
        // get a serializer for the response and set the response to it
        ServerIdentifierSetSerializer responseSerializer = new ServerIdentifierSetSerializer();
        responseSerializer.setIdentifiers(members);
        // Now send the response
        sendResponse(requestPacket, responseSerializer, false);
    }
}
