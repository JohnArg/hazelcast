package discovery.service.rpc.invocators;

import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.BooleanSerializer;
import discovery.service.api.DiscoveryApiImpl;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class UnregisterServerApiInvocatorTest {

    @Test
    @Tag("Invocations")
    @DisplayName("Invoking unregisterServer API Test")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testApiCall(){
        ExecutorService workers = Executors.newFixedThreadPool(2);
        DiscoveryApiImpl api = new DiscoveryApiImpl();
        UnregisterServerApiInvocator apiInvocator = new UnregisterServerApiInvocator(workers, api);
        MockPacketFactory packetFactory = new MockPacketFactory(200);

        ServerIdentifier identifier = new ServerIdentifier();
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(new InetSocketAddress(3000));
        // try with no-registered identifiers
        apiInvokerTest(api, packetFactory, apiInvocator, identifier, false);
        // try registering the identifier first
        api.registerServer(identifier);
        apiInvokerTest(api, packetFactory, apiInvocator, identifier, true);
        // try again to unregister from empty set
        apiInvokerTest(api, packetFactory, apiInvocator, identifier, false);
        // try to unregister from non-empty set when the identifier exists
        ServerIdentifier identifier2 = new ServerIdentifier();
        identifier.setRdmaAddress(new InetSocketAddress(4000));
        identifier.setTcpAddress(new InetSocketAddress(5000));
        ServerIdentifier identifier3 = new ServerIdentifier();
        identifier.setRdmaAddress(new InetSocketAddress(6000));
        identifier.setTcpAddress(new InetSocketAddress(6000));
        api.registerServer(identifier2);
        api.registerServer(identifier);
        api.registerServer(identifier3);
        apiInvokerTest(api, packetFactory, apiInvocator, identifier, true);
        // try to unregister from non-empty set when the identifier does not exist
        apiInvokerTest(api, packetFactory, apiInvocator, identifier, false);
    }

    private void apiInvokerTest(DiscoveryApiImpl api, MockPacketFactory packetFactory,
                                UnregisterServerApiInvocator apiInvocator, ServerIdentifier identifier,
                                boolean expectedResponse){
        CompletableFuture<ByteBuffer> responseFuture = new CompletableFuture<>();
        DiscoveryPacket requestPacket = packetFactory.get(responseFuture);
        requestPacket.setMessageType(RpcMessageType.REQUEST)
                .setOperationType(DiscoveryOperationType.UNREGISTER_SERVER)
                .setOperationId(123)
                .setPacketNumber(0);

        identifier.setWorkRequestProxy(requestPacket.getWorkRequestProxy());
        assertDoesNotThrow(()->{
            identifier.writeToWorkRequestBuffer();
            requestPacket.getWorkRequestProxy().getBuffer().flip();
        });

        // Run and wait for response to be "sent"
        assertDoesNotThrow(()->{apiInvocator.invokeOperation(requestPacket);});
        try {
            responseFuture.get();    // don't need the result
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }

        // the response was saved in the same Work Request proxy due to mocking
        // let's deserialize and read it
        DiscoveryPacket responsePacket = new DiscoveryPacket(requestPacket.getWorkRequestProxy());
        assertDoesNotThrow(()->{responsePacket.readHeadersFromWorkRequestBuffer();});
        BooleanSerializer boolSerializer = new BooleanSerializer(responsePacket
                .getWorkRequestProxy());
        assertDoesNotThrow(()->{boolSerializer.readFromWorkRequestBuffer();});
        boolean response = boolSerializer.getFlag();
        assertEquals(expectedResponse, response);
    }
}
