package discovery.common;

import jarg.jrcm.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.jrcm.rpc.packets.PacketDispatcher;

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
