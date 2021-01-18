package discovery.client.rpc.response.invocators;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.serializers.BooleanSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class UnregisterServerResponseInvocatorTest {

    @ParameterizedTest
    @Tag("ResponseInvocation")
    @DisplayName("Testing correctness of response invocator.")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    @ValueSource(booleans = {true, false})
    public void responseInvocationTest(boolean responseValue){
        // We need a mock packet from the appropriate factory
        MockPacketFactory packetFactory = new MockPacketFactory(200);
        // And a way to detect when the RdmaCommunicator associated with the packet
        // sends a request to the NIC
        CompletableFuture<ByteBuffer> netRequestFuture = new CompletableFuture<>();
        DiscoveryPacket packet = packetFactory.get(netRequestFuture);
        // We also need to create a Future that the invocator will complete with the
        // response
        CompletableFuture<Boolean> membersResponse = new CompletableFuture<>();
        // Let's put that Future to a response manager
        PendingResponseManager responseManager = new PendingResponseManager();
        responseManager.unregisterServerPendingResponses().put(0L, membersResponse);
        // We can now create an invocator to get the registered servers and pass it this manager.
        UnregisterServerResponseInvocator invocator = new UnregisterServerResponseInvocator(responseManager);
        // let's prepare the packet to send to the invocator
        BooleanSerializer serializer = new BooleanSerializer();
        serializer.setFlag(responseValue);
        serializer.setWorkRequestProxy(packet.getWorkRequestProxy());
        assertDoesNotThrow(()->{packet.writeToWorkRequestBuffer(serializer);});
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
            Boolean invocatorValue = membersResponse.get();
            // check results
            assertEquals(responseValue, invocatorValue);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

}
