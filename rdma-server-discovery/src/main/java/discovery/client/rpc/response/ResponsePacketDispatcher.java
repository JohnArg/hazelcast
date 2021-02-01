package discovery.client.rpc.response;

import discovery.client.rpc.response.invocators.GetRegisteredServersResponseInvocator;
import discovery.client.rpc.response.invocators.RegisterServerResponseInvocator;
import discovery.client.rpc.response.invocators.UnregisterServerResponseInvocator;
import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used after deserializing the headers of a received ${@link DiscoveryPacket}.
 * It reads those headers to identify the type of received response and call the appropriate
 * response handler.
 */
public class ResponsePacketDispatcher implements PacketDispatcher<DiscoveryPacket> {
    private final Logger logger = LoggerFactory.getLogger(ResponsePacketDispatcher.class);

    private PendingResponseManager responseManager;
    // response invocators
    private RegisterServerResponseInvocator registerServerResponseInvocator;
    private UnregisterServerResponseInvocator unregisterServerResponseInvocator;
    private GetRegisteredServersResponseInvocator getRegisteredServersResponseInvocator;

    public ResponsePacketDispatcher(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        registerServerResponseInvocator = new RegisterServerResponseInvocator(responseManager);
        unregisterServerResponseInvocator = new UnregisterServerResponseInvocator(responseManager);
        getRegisteredServersResponseInvocator = new GetRegisteredServersResponseInvocator(responseManager);
    }

    @Override
    public void dispatchPacket(DiscoveryPacket packet) {
        // deserialize packet headers
        try {
            packet.readHeadersFromWorkRequestBuffer();
        } catch (RpcDataSerializationException e) {
            logger.error("Could not deserialize packet headers.", e);
            return;
        }
        // dispatch according to headers
        switch(packet.getOperationType()){
            case DiscoveryOperationType.REGISTER_SERVER:
                registerServerResponseInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.UNREGISTER_SERVER:
                unregisterServerResponseInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVERS:
                getRegisteredServersResponseInvocator.invokeOperation(packet);
                break;
            default:
        }
    }

    public PendingResponseManager getResponseManager() {
        return responseManager;
    }
}
