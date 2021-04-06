package discovery.common;

import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.jrcm.networking.dependencies.netrequests.types.PostedRequestType;
import jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Supplies mock {@link WorkRequestProxy} objects for testing.
 */
public class MockWorkRequestProxyFactory{

    private int netBufferSize;

    public MockWorkRequestProxyFactory(int netBufferSize) {
        this.netBufferSize = netBufferSize;
    }

    /**
     * <p>
     * Provides a Work Request Proxy that can be used for both sending a
     * request and receiving a response. This helps in testing
     * API invocators that call the Discovery API and return a response.
     * Instead of getting different Work Request proxies for a request
     * and a response, as would normally be the case, this factory allows
     * you to reuse the same object for testing.
     * </p>
     * <p>
     * The {@link CompletableFuture} parameter will be used to complete
     * a Future once the response is sent to the remote side, which happens
     * in another thread. Due to testing,
     * no such sending will actually take place, but a mock representation of
     * an {@link jarg.jrcm.networking.communicators.RdmaCommunicator RdmaCommunicator}
     * will simply complete the Future by passing the Work Request proxy's buffer, now
     * containing the response bytes.
     * So, in summary, the {@link CompletableFuture} serves the purpose of waiting until
     * the response is sent by another thread, before being able to test the response's
     * data for correctness.
     * </p>
     *
     * @param responseFuture the future signaling that a response was sent.
     * @return the WorkRequestProxy that can be used in testing.
     */
    public WorkRequestProxy get(CompletableFuture<ByteBuffer> responseFuture) {
        // create test buffer
        ByteBuffer packetBuffer = ByteBuffer.allocate(netBufferSize);
        // mock a proxy provider
        QueuedProxyProvider mockProxyProvider = mock(QueuedProxyProvider.class);
        // mock RDMA communicator
        ActiveRdmaCommunicator mockCommunicator = mock(ActiveRdmaCommunicator.class);
        when(mockCommunicator.getWorkRequestProxyProvider()).thenReturn(mockProxyProvider);
        // create the test WR proxy that will be returned
        WorkRequestProxy mockProxy = new WorkRequestProxy(-1, PostedRequestType.SEND,
                WorkRequestType.TWO_SIDED_SEND_SIGNALED, packetBuffer,
                mockCommunicator);
        // if the same communicator is used to send a response, complete the Future parameter
        // this way we can wait until the response is "sent", to test if its results are correct
        when(mockCommunicator.postNetOperationToNIC(any(WorkRequestProxy.class))).then(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                return responseFuture.complete(mockProxy.getBuffer());
            }
        });
        // In some cases the communicator of the WR proxy will be used to get a new WR.
        // We'll just return the mockProxy in these cases too. No need for new objects.
        Answer<WorkRequestProxy> proxyProvision = new Answer<WorkRequestProxy>() {
            @Override
            public WorkRequestProxy answer(InvocationOnMock invocation) throws Throwable {
                mockProxy.getBuffer().clear();
                return mockProxy;
            }
        };
        when(mockProxyProvider.getPostSendRequestBlocking(any(WorkRequestType.class)))
                .then(proxyProvision);
        when(mockProxyProvider.getPostSendRequestNow(any(WorkRequestType.class)))
                .then(proxyProvision);
        return mockProxy;
    }
}
