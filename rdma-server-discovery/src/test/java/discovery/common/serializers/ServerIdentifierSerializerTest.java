package discovery.common.serializers;

import discovery.common.api.ServerIdentifier;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServerIdentifierSerializerTest {

    @Test
    @Tag("Serialization")
    @DisplayName("Server Identifier Serialization & Deserialization Test")
    public void serverIdentifierTest(){
        ServerIdentifier identifier = new ServerIdentifier();
        ByteBuffer testBuffer = ByteBuffer.allocate(50);
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, null,
                null, testBuffer, null);
        identifier.setWorkRequestProxy(mockProxy);

        // Both addresses Set -------------------------
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(new InetSocketAddress(4000));
        assertDoesNotThrow( ()->{ identifier.writeToWorkRequestBuffer(); });
        // deserialize now
        testBuffer.flip();
        assertDoesNotThrow(()->{identifier.readFromWorkRequestBuffer();});
        assertEquals(identifier, identifier);
        // RDMA address Set -------------------------
        testBuffer.clear();
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(null);
        assertDoesNotThrow( ()->{ identifier.writeToWorkRequestBuffer(); });
        // deserialize now
        testBuffer.flip();
        assertDoesNotThrow(()->{identifier.readFromWorkRequestBuffer();});
        assertEquals(identifier, identifier);
        // TCP address Set -------------------------
        testBuffer.clear();
        identifier.setRdmaAddress(null);
        identifier.setTcpAddress(new InetSocketAddress(3000));
        assertDoesNotThrow( ()->{ identifier.writeToWorkRequestBuffer(); });
        // deserialize now
        testBuffer.flip();
        assertDoesNotThrow(()->{identifier.readFromWorkRequestBuffer();});
        assertEquals(identifier, identifier);
        // Both addresses null -------------------------
        testBuffer.clear();
        identifier.setRdmaAddress(null);
        identifier.setTcpAddress(null);
        assertDoesNotThrow( ()->{ identifier.writeToWorkRequestBuffer(); });
        // deserialize now
        testBuffer.flip();
        assertDoesNotThrow(()->{identifier.readFromWorkRequestBuffer();});
        assertEquals(identifier, identifier);
    }

    @Test
    @Tag("Serialization")
    @DisplayName("Server Identifier Set Serialization & Deserialization Test")
    public void serverIdentifierSetTest(){
        // Initialize Set
        Set<ServerIdentifier> serverIdentifiers = new HashSet<>();
        serverIdentifiers.add(new ServerIdentifier(new InetSocketAddress(3000),
                new InetSocketAddress(4000)));
        serverIdentifiers.add(new ServerIdentifier(null,
                new InetSocketAddress(4000)));
        serverIdentifiers.add(new ServerIdentifier(new InetSocketAddress(3000),
                null));
        serverIdentifiers.add(new ServerIdentifier(null, null));
        // Set WR buffer
        ByteBuffer testBuffer = ByteBuffer.allocate(12 + 4*(2*(4+4) + 8));
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, null,
                null, testBuffer, null);

        ServerIdentifierSetSerializer setSerializer = new ServerIdentifierSetSerializer(mockProxy);
        setSerializer.setIdentifiers(serverIdentifiers);

        assertDoesNotThrow(()->{setSerializer.writeToWorkRequestBuffer();});
        testBuffer.flip();
        assertDoesNotThrow(()->{setSerializer.readFromWorkRequestBuffer();});
        assertEquals(serverIdentifiers, setSerializer.getIdentifiers());
    }
}
