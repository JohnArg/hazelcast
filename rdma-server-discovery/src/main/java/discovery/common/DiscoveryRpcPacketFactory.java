package discovery.common;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.packets.RpcPacketFactory;
import jarg.jrcm.rpc.request.RequestIdGenerator;

/**
 * A factory for server discovery RPC packets.
 */
public class DiscoveryRpcPacketFactory implements RpcPacketFactory<DiscoveryPacket> {

    private RequestIdGenerator<Long> requestIdGenerator;

    public DiscoveryRpcPacketFactory(RequestIdGenerator<Long> requestIdGenerator) {
        this.requestIdGenerator = requestIdGenerator;
    }

    @Override
    public DiscoveryPacket generatePacket(WorkRequestProxy workRequestProxy,
                                             byte messageType, int operationType) {
        DiscoveryPacket packet = new DiscoveryPacket(workRequestProxy);
        // Create the RPC packet headers
        packet.setMessageType(messageType)
                .setOperationType(operationType)
                .setOperationId(requestIdGenerator.generateRequestId())
                .setPacketNumber(0);
        // Create RPC packet
        return packet;
    }
}
