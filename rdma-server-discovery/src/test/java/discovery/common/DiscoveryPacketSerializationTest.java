package discovery.common;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.packets.RpcMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DiscoveryPacketSerializationTest {

    @Test
    @Tag("Serialization")
    @DisplayName("Discovery Packet Serialization & Deserialization Test")
    public void discoveryPacketSerializationTest(){
        ByteBuffer testBuffer = ByteBuffer.allocate(2*8+1+2*4);
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, null,
                null, testBuffer, null);
        DiscoveryPacket packet = new DiscoveryPacket(mockProxy);

        assertDoesNotThrow(()->{packet.writeToWorkRequestBuffer(null);});
        testBuffer.flip();
        assertDoesNotThrow(()->{packet.readHeadersFromWorkRequestBuffer();});
        assertEquals(packet, packet);

        packet.setMessageType(RpcMessageType.REQUEST)
                .setOperationId(124)
                .setOperationType(DiscoveryOperationType.GET_SERVERS)
                .setPacketNumber(0);

        testBuffer.clear();
        assertDoesNotThrow(()->{packet.writeToWorkRequestBuffer(null);});
        testBuffer.flip();
        assertDoesNotThrow(()->{packet.readHeadersFromWorkRequestBuffer();});
        assertEquals(packet, packet);
    }
}
