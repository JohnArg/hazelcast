package discovery.service.rpc.invocators;

import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import discovery.service.api.DiscoveryApiImpl;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
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

public class GetRegisteredServersApiInvocatorTest {

    @Test
    @Tag("Invocations")
    @DisplayName("Invoking getRegisteredServers API Test")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void testApiCall(){
        ExecutorService workers = Executors.newFixedThreadPool(2);
        DiscoveryApiImpl api = new DiscoveryApiImpl();
        GetRegisteredServersApiInvocator apiInvocator = new GetRegisteredServersApiInvocator(workers, api);
        MockPacketFactory packetFactory = new MockPacketFactory(200);

        // try with no registered servers --------------
        apiInvokerTest(api, packetFactory, apiInvocator);
        // register some servers and try again ----------
        ServerIdentifier server1 = new ServerIdentifier();
        ServerIdentifier server2 = new ServerIdentifier();
        ServerIdentifier server3 = new ServerIdentifier();
        server1.setRdmaAddress(new InetSocketAddress(3000));
        server1.setTcpAddress(new InetSocketAddress(3000));
        server2.setRdmaAddress(new InetSocketAddress(4000));
        server2.setTcpAddress(new InetSocketAddress(4000));
        server3.setRdmaAddress(new InetSocketAddress(6000));
        server3.setTcpAddress(new InetSocketAddress(6000));
        api.registerServer(server1);
        api.registerServer(server2);
        api.registerServer(server3);

        apiInvokerTest(api, packetFactory, apiInvocator);
        // unregister a server and try ----------------------
        api.unregisterServer(server2);
        apiInvokerTest(api, packetFactory, apiInvocator);

        workers.shutdown();
    }

    private void apiInvokerTest(DiscoveryApiImpl api, MockPacketFactory packetFactory,
                                GetRegisteredServersApiInvocator apiInvocator){
        CompletableFuture<ByteBuffer> responseFuture = new CompletableFuture<>();
        DiscoveryPacket requestPacket = packetFactory.get(responseFuture);
        requestPacket.setMessageType(RpcMessageType.REQUEST)
                .setOperationType(DiscoveryOperationType.GET_SERVERS)
                .setOperationId(123)
                .setPacketNumber(0);

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
        assertEquals(api.getRegisteredServers(), identifiers);
    }

}
