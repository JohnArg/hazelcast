package discovery.common;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * A {@link Supplier} of {@link DiscoveryPacket DiscoveryPackets} for test.
 */
public class MockPacketFactory{

    private int packetBufferSize;
    private MockWorkRequestProxyFactory workRequestProxyFactory;

    public MockPacketFactory(int packetBufferSize){
        this.packetBufferSize = packetBufferSize;
        workRequestProxyFactory = new MockWorkRequestProxyFactory(packetBufferSize);
    }

    public DiscoveryPacket get(CompletableFuture<ByteBuffer> responseFuture) {
        WorkRequestProxy mockProxy = workRequestProxyFactory.get(responseFuture);
        DiscoveryPacket packet = new DiscoveryPacket(mockProxy);
        return packet;
    }
}
