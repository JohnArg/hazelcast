package discovery.service.rpc.invocators;

import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import discovery.service.api.DiscoveryApiImpl;
import jarg.rdmarpc.rpc.exception.RpcExecutionException;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class RegisterServerApiInvocatorTest {


    @Test
    @Tag("Invocations")
    @DisplayName("Invoking registerServer API Test")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testApiCall(){
        DiscoveryApiImpl api = new DiscoveryApiImpl();
        RegisterServerApiInvocator  apiInvocator = new RegisterServerApiInvocator(api);
        MockPacketFactory packetFactory = new MockPacketFactory(200);

        // try with no registered servers --------------
        ServerIdentifier identifier = new ServerIdentifier();
        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(new InetSocketAddress(3000));
        apiInvokerTest(api, packetFactory, apiInvocator, identifier);

        identifier.setRdmaAddress(new InetSocketAddress(3000));
        identifier.setTcpAddress(null);
        apiInvokerTest(api, packetFactory, apiInvocator, identifier);

        identifier.setRdmaAddress(null);
        identifier.setTcpAddress(new InetSocketAddress(3000));
        apiInvokerTest(api, packetFactory, apiInvocator, identifier);

    }

    private void apiInvokerTest(DiscoveryApiImpl api, MockPacketFactory packetFactory,
                                RegisterServerApiInvocator apiInvocator, ServerIdentifier identifier){
        CompletableFuture<ByteBuffer> responseFuture = new CompletableFuture<>();
        DiscoveryPacket requestPacket = packetFactory.get(responseFuture);
        requestPacket.setMessageType(RpcMessageType.REQUEST)
                .setOperationType(DiscoveryOperationType.REGISTER_SERVER)
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
        ServerIdentifierSetSerializer setSerializer = new ServerIdentifierSetSerializer(responsePacket
                .getWorkRequestProxy());
        assertDoesNotThrow(()->{setSerializer.readFromWorkRequestBuffer();});
        Set<ServerIdentifier> identifiers = setSerializer.getIdentifiers();
        Set<ServerIdentifier> expectedServers = null;
        try {
            expectedServers = api.getRegisteredServers();
        } catch (RpcExecutionException e) {
            e.printStackTrace();
            fail();
        }
        assertEquals(expectedServers, identifiers);
    }
}
