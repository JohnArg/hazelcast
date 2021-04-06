package discovery.client.rpc.response;

import discovery.client.rpc.DiscoveryServiceProxy;
import discovery.client.rpc.request.DiscoveryRequestIdGenerator;
import discovery.common.DiscoveryPacket;
import discovery.common.MockPacketFactory;
import discovery.common.api.ServerIdentifier;
import discovery.service.api.DiscoveryApiImpl;
import jarg.jrcm.networking.communicators.RdmaCommunicator;
import jarg.jrcm.rpc.exception.RpcExecutionException;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


public class DiscoveryServiceProxyTest {

    private DiscoveryApiImpl discoveryApi;
    private MockPacketFactory mockPacketFactory;
    private RdmaCommunicator mockCommunicator;
    private DiscoveryPacket mockPacket;
    private PendingResponseManager pendingResponseManager;
    private DiscoveryServiceProxy discoveryServiceProxy;
    private CompletableFuture<ByteBuffer> netRequestPostedFuture;
    private static Set<ServerIdentifier> identifiersToTest;
    private static final int timeout = 3000;

    @BeforeAll
    public static void beforeAll(){
        // we need to test them all at the same test
        // that's why we won't use parametrized tests
        identifiersToTest = new HashSet<>();
        ServerIdentifier server1 = new ServerIdentifier(
                new InetSocketAddress(3000),
                new InetSocketAddress(3000)
        );
        ServerIdentifier server2 = new ServerIdentifier(
                null,
                new InetSocketAddress(3000)
        );
        ServerIdentifier server3 = new ServerIdentifier(
                new InetSocketAddress(3000),
                null
        );
        ServerIdentifier server4 = new ServerIdentifier(
                null,
                null
        );
        identifiersToTest.add(server1);
        identifiersToTest.add(server2);
        identifiersToTest.add(server3);
        identifiersToTest.add(server4);
    }

    @BeforeEach
    public void beforeEach(){
        discoveryApi = new DiscoveryApiImpl();
        mockPacketFactory = new MockPacketFactory(200);
        pendingResponseManager = new PendingResponseManager();
    }

    // Will wait for a message to be sent to the NIC and then
    // mocks a response to the sender.
    private abstract class ResponseThread extends Thread{

        private CompletableFuture<ByteBuffer> netRequestPostedFuture;
        private ServerIdentifier identifier;

        public ResponseThread(CompletableFuture<ByteBuffer> netRequestPostedFuture,
                              ServerIdentifier identifier) {
            this.netRequestPostedFuture = netRequestPostedFuture;
            this.identifier = identifier;
        }

        @Override
        public void run(){
            try {
                // wait for a message to send to the NIC
                netRequestPostedFuture.get();
                handleResponse();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        public abstract void handleResponse();
    }

    @Test
    @Tag("RemoteApiCall")
    @DisplayName("Testing registerServer remote API Call.")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void registerServerTest(){
        // Prepare expected results
        Set<ServerIdentifier> expectedMembers = new HashSet<>();

        for(ServerIdentifier identifier : identifiersToTest){
            // generate dependencies for the discovery proxy
            netRequestPostedFuture = new CompletableFuture<>();
            mockPacket = mockPacketFactory.get(netRequestPostedFuture);
            mockCommunicator = mockPacket.getWorkRequestProxy().getRdmaCommunicator();
            discoveryServiceProxy = new DiscoveryServiceProxy(mockCommunicator,
                    pendingResponseManager, new DiscoveryRequestIdGenerator(0), timeout);
            // define how to handle responses
            ResponseThread responseThread = new ResponseThread(netRequestPostedFuture, identifier) {
                @Override
                public void handleResponse() {
                    // make sure that the request was posted
                    try {
                        netRequestPostedFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    CompletableFuture<Set<ServerIdentifier>> identifiers =
                            pendingResponseManager.registerServerPendingResponses().remove(0L);
                    // "send" a response
                    try {
                        identifiers.complete(discoveryApi.registerServer(identifier));
                    } catch (RpcExecutionException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            };
            // start the response thread before invoking the API
            responseThread.start();
            // invoke the proxy API
            Set<ServerIdentifier> members = null;
            try {
                members = discoveryServiceProxy.registerServer(identifier);
            } catch (RpcExecutionException e) {
                e.printStackTrace();
                fail();
            }
            // test results of proxy API invocation
            expectedMembers.add(identifier);
            assertEquals(expectedMembers, members);
        }
    }

    @Test
    @Tag("RemoteApiCall")
    @DisplayName("Testing unregisterServer remote API Call.")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void unregisterServerTest(){
        // try both an empty and a non-empty initial set of identifiers
        for(int i=0; i<2; i++){
            // the second time, fill the set
            if(i == 1){
                for(ServerIdentifier identifier : identifiersToTest) {
                    try {
                        discoveryApi.registerServer(identifier);
                    } catch (RpcExecutionException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }
            // run the tests
            for(ServerIdentifier identifier : identifiersToTest) {
                // generate dependencies for the discovery proxy
                netRequestPostedFuture = new CompletableFuture<>();
                mockPacket = mockPacketFactory.get(netRequestPostedFuture);
                mockCommunicator = mockPacket.getWorkRequestProxy().getRdmaCommunicator();
                discoveryServiceProxy = new DiscoveryServiceProxy(mockCommunicator,
                        pendingResponseManager, new DiscoveryRequestIdGenerator(0), timeout);
                // define how to handle responses
                ResponseThread responseThread = new ResponseThread(netRequestPostedFuture, identifier) {
                    @Override
                    public void handleResponse() {
                        // make sure that the request was posted
                        try {
                            netRequestPostedFuture.get();
                        } catch (InterruptedException | ExecutionException e) {
                            e.printStackTrace();
                        }
                        CompletableFuture<Boolean> flags =
                                pendingResponseManager.unregisterServerPendingResponses().remove(0L);
                        // "send" a response
                        try {
                            flags.complete(discoveryApi.unregisterServer(identifier));
                        } catch (RpcExecutionException e) {
                            e.printStackTrace();
                            fail();
                        }
                    }
                };
                // start the response thread before invoking the API
                responseThread.start();
                // invoke the proxy API
                boolean success = false;
                try {
                    success = discoveryServiceProxy.unregisterServer(identifier);
                } catch (RpcExecutionException e) {
                    e.printStackTrace();
                    fail();
                }
                // test results of proxy API invocation
                boolean expectedSuccess = (i == 1);
                assertEquals(expectedSuccess, success);
            }
        }
    }

    @Test
    @Tag("RemoteApiCall")
    @DisplayName("Testing getServers remote API Call.")
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    public void getServersTest(){
        // Prepare expected results
        Set<ServerIdentifier> expectedMembers = new HashSet<>();
        // try both from an empty and a non-empty set
        for(int i=0; i<2; i++){
            // the second time, fill the set
            if(i == 1){
                for(ServerIdentifier identifier : identifiersToTest) {
                    try {
                        discoveryApi.registerServer(identifier);
                    } catch (RpcExecutionException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }
            // generate dependencies for the discovery proxy
            netRequestPostedFuture = new CompletableFuture<>();
            mockPacket = mockPacketFactory.get(netRequestPostedFuture);
            mockCommunicator = mockPacket.getWorkRequestProxy().getRdmaCommunicator();
            discoveryServiceProxy = new DiscoveryServiceProxy(mockCommunicator,
                    pendingResponseManager, new DiscoveryRequestIdGenerator(0), timeout);
            // define how to handle responses
            ResponseThread responseThread = new ResponseThread(netRequestPostedFuture, null) {
                @Override
                public void handleResponse() {
                    // make sure that the request was posted
                    try {
                        netRequestPostedFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                    CompletableFuture<Set<ServerIdentifier>> identifiers =
                            pendingResponseManager.getRegisteredServersPendingResponses().remove(0L);
                    // "send" a response
                    try {
                        identifiers.complete(discoveryApi.getRegisteredServers());
                    } catch (RpcExecutionException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            };
            // start the response thread before invoking the API
            responseThread.start();
            // invoke the proxy API
            Set<ServerIdentifier> members = null;
            try {
                members = discoveryServiceProxy.getRegisteredServers();
            } catch (RpcExecutionException e) {
                e.printStackTrace();
                fail();
            }
            try {
                expectedMembers = discoveryApi.getRegisteredServers();
            } catch (RpcExecutionException e) {
                e.printStackTrace();
                fail();
            }
            // test results of proxy API invocation
            assertEquals(expectedMembers, members);
        }
    }

}
