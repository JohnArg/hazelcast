package discovery.service.rpc.invocators;

import discovery.common.DiscoveryPacket;
import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.BooleanSerializer;
import discovery.service.rpc.response.SinglePacketResponseTask;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.invocation.AbstractThreadPoolInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Reads information from an {@link DiscoveryPacket}, deserializes parameters and invokes
 * {@link DiscoveryApi#unregisterServer(ServerIdentifier)}. It then sends back a response to
 * the caller.
 */
public class UnregisterServerApiInvocator extends AbstractThreadPoolInvocator {
    private static final Logger logger = LoggerFactory.getLogger(UnregisterServerApiInvocator.class.getSimpleName());

    private DiscoveryApi serviceApi;

    public UnregisterServerApiInvocator(ExecutorService workersExecutor, DiscoveryApi serviceApi) {
       super(workersExecutor);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperationTask(AbstractRpcPacket packet){
        // Pass the packet's work request data to the serializer
        WorkRequestProxy workRequestData = packet.getWorkRequestProxy();
        ServerIdentifier serverIdentifier = new ServerIdentifier();
        serverIdentifier.setWorkRequestProxy(workRequestData);

        try {
            // deserialize request parameters from the received packet
            serverIdentifier.readFromWorkRequestBuffer();
            // Free WR id, we have the objects we need
            workRequestData.releaseWorkRequest();
            // invoke the service's API
            boolean success = serviceApi.unregisterServer(serverIdentifier);
            // get a serializer for the response and set the response to it
            BooleanSerializer responseSerializer = new BooleanSerializer();
            responseSerializer.setFlag(success);
            // send the response to the caller in another task
            SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryPacket) packet,
                    responseSerializer, TWO_SIDED_SEND_SIGNALED, false);
            getWorkersExecutor().submit(responseTask);
        } catch (RpcDataSerializationException e) {
            // Free WR id
            workRequestData.releaseWorkRequest();
            // send the response to the caller in another task
            SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryPacket) packet,
                    null, TWO_SIDED_SEND_SIGNALED,true);
            getWorkersExecutor().submit(responseTask);
            logger.error("Unable to invoke service API", e);
        }
    }
}
