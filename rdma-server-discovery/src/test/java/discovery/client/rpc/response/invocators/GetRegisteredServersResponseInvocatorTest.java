package discovery.client.rpc.response.invocators;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryOperationType;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.api.ServerIdentifier;
import discovery.common.serializers.ServerIdentifierSetSerializer;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class GetRegisteredServersResponseInvocatorTest {

    @Test
    @Tag("ResponseInvocation")
    @DisplayName("Testing correctness of response invocator.")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void responseInvocationTest(){
        // We need a mock packet from the appropriate factory
        MockPacketFactory packetFactory = new MockPacketFactory(200);
        // And a way to detect when the RdmaCommunicator associated with the packet
        // sends a request to the NIC
        CompletableFuture<ByteBuffer> netRequestFuture = new CompletableFuture<>();
        DiscoveryPacket packet = packetFactory.get(netRequestFuture);
        // We also need to create a Future that the invocator will complete with the
        // response
        CompletableFuture<Set<ServerIdentifier>> membersResponse = new CompletableFuture<>();
        // Let's put that Future to a response manager
        PendingResponseManager responseManager = new PendingResponseManager();
        responseManager.getRegisteredServersPendingResponses().put(0L, membersResponse);
        // We can now create an invocator to get the registered servers and pass it this manager.
        GetRegisteredServersResponseInvocator invocator = new GetRegisteredServersResponseInvocator(responseManager);
        // let's prepare the packet to send to the invocator
        Set<ServerIdentifier> members = new HashSet<>();
        members.add(new ServerIdentifier(new InetSocketAddress(3000), new InetSocketAddress(3000)));
        members.add(new ServerIdentifier(new InetSocketAddress(4000), new InetSocketAddress(4000)));
        members.add(new ServerIdentifier(new InetSocketAddress(5000), new InetSocketAddress(5000)));
        ServerIdentifierSetSerializer setSerializer = new ServerIdentifierSetSerializer();
        setSerializer.setIdentifiers(members);
        setSerializer.setWorkRequestProxy(packet.getWorkRequestProxy());
        assertDoesNotThrow(()->{packet.writeToWorkRequestBuffer(setSerializer);});
        // We just wrote to the WR buffer, let's flip it for this to work
        packet.getWorkRequestProxy().getBuffer().flip();
        // we also have to read the headers for this to work - that's what a PacketDispatcher does
        // before calling the appropriate invocator
        assertDoesNotThrow(()->{packet.readHeadersFromWorkRequestBuffer();});
        // Now let's call the invocator on another thread, passing the packet
        Thread invocatorThread = new Thread(()->{
            invocator.invokeOperation(packet);
        });
        invocatorThread.start();
        // Wait for the response
        try {
            Set<ServerIdentifier> receivedMembers = membersResponse.get();
            // check results
            assertEquals(members, receivedMembers);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

}
