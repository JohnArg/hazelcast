package discovery.service.rpc;

import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.api.DiscoveryApi;
import discovery.service.rpc.invocators.GetRegisteredServersApiInvocator;
import discovery.service.rpc.invocators.RegisterServerApiInvocator;
import discovery.service.rpc.invocators.UnregisterServerApiInvocator;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.invocation.RpcOperationInvocator;
import jarg.jrcm.rpc.packets.PacketDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Used after deserializing the headers of a received ${@link DiscoveryPacket}.
 * It reads those headers to identify the type of received request and call the appropriate
 * ${@link RpcOperationInvocator}.
 * Then, the invocator will call the service's API that corresponds to packet's operation type.
 */
public class DiscoveryServicePacketDispatcher implements PacketDispatcher<DiscoveryPacket> {

    private final Logger logger = LoggerFactory.getLogger(DiscoveryServicePacketDispatcher.class);

    private RpcOperationInvocator registerServerApiInvocator;
    private RpcOperationInvocator unregisterServerApiInvocator;
    private RpcOperationInvocator getRegisteredServersApiInvocator;

    public DiscoveryServicePacketDispatcher(DiscoveryApi rdmaDiscoveryApi) {
        this.registerServerApiInvocator = new RegisterServerApiInvocator(rdmaDiscoveryApi);
        this.unregisterServerApiInvocator = new UnregisterServerApiInvocator(rdmaDiscoveryApi);
        this.getRegisteredServersApiInvocator = new GetRegisteredServersApiInvocator(rdmaDiscoveryApi);
    }

    @Override
    public void dispatchPacket(DiscoveryPacket packet) {
        // read headers from packet data
        try {
            packet.readHeadersFromWorkRequestBuffer();
        } catch (RpcDataSerializationException e) {
            logger.error("Could not deserialize packet headers.", e);
            return;
        }
        // dispatch according to headers
        switch(packet.getOperationType()){
            case DiscoveryOperationType.REGISTER_SERVER:
                registerServerApiInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.UNREGISTER_SERVER:
                unregisterServerApiInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVERS:
                getRegisteredServersApiInvocator.invokeOperation(packet);
                break;
            default:
        }
    }
}
