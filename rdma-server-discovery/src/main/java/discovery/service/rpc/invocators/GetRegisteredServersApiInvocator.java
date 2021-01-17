package discovery.service.rpc.invocators;

import discovery.common.DiscoveryPacket;
import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import discovery.service.rpc.response.SinglePacketResponseTask;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.invocation.AbstractThreadPoolInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Reads information from an {@link DiscoveryPacket}, deserializes parameters and invokes
 * {@link DiscoveryApi#getRegisteredServers()}. It then sends back a response to
 * the caller.
 */
public class GetRegisteredServersApiInvocator extends AbstractThreadPoolInvocator {

    private static final Logger logger = LoggerFactory.getLogger(GetRegisteredServersApiInvocator.class.getSimpleName());

    private DiscoveryApi serviceApi;

    public GetRegisteredServersApiInvocator(ExecutorService workersExecutor, DiscoveryApi serviceApi) {
        super(workersExecutor);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperationTask(AbstractRpcPacket packet) {
        // Get the packet's work request data
        WorkRequestProxy workRequestProxy = packet.getWorkRequestProxy();
        // Free WR id, we have the objects we need
        workRequestProxy.releaseWorkRequest();
        // invoke the service's API
        Set<ServerIdentifier> members = serviceApi.getRegisteredServers();
        // get a serializer for the response and set the response to it
        ServerIdentifierSetSerializer responseSerializer = new ServerIdentifierSetSerializer();
        responseSerializer.setIdentifiers(members);
        // send the response to the caller in another task
        SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryPacket) packet,
                responseSerializer, TWO_SIDED_SEND_SIGNALED, false);
        getWorkersExecutor().submit(responseTask);
    }
}
