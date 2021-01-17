package discovery.common;

import jarg.rdmarpc.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;

public class DiscoveryCommunicatorDependencies extends RdmaCommunicatorDependencies {

    private PacketDispatcher<DiscoveryPacket> packetDispatcher;

    public PacketDispatcher<DiscoveryPacket> getPacketDispatcher() {
        return packetDispatcher;
    }

    public DiscoveryCommunicatorDependencies setPacketDispatcher(PacketDispatcher<DiscoveryPacket>
                                                                         packetDispatcher) {
        this.packetDispatcher = packetDispatcher;
        return this;
    }
}
