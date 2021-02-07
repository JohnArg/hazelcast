package discovery.client.networking;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import discovery.client.rpc.response.PendingResponseManager;
import discovery.client.rpc.response.ResponsePacketDispatcher;
import discovery.common.networking.NetworkCompletionEventHandler;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netbuffers.impl.TwoSidedBufferManager;
import jarg.rdmarpc.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.rdmarpc.networking.dependencies.svc.impl.TwoSidedSVCManager;

import java.io.IOException;

/**
 * A factory of {@link ActiveRdmaCommunicator} objects for the Discovery RPC client.
 */
public class ClientCommunicatorFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;

    public ClientCommunicatorFactory(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                     int maxBufferSize, int maxWorkRequests) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWorkRequests = maxWorkRequests;
    }

    @Override
    public ActiveRdmaCommunicator createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        // Create a pending response manager
        PendingResponseManager responseManager = new PendingResponseManager();
        // Create a packet dispatcher for this endpoint and pass the above response manager
        ResponsePacketDispatcher packetDispatcher = new ResponsePacketDispatcher(responseManager);
        // Create endpoint dependencies
        ClientCommunicatorDependencies dependencies = new ClientCommunicatorDependencies();
        dependencies.setResponseManager(responseManager);
        dependencies.setPacketDispatcher(packetDispatcher);
        dependencies.setMaxBufferSize(maxBufferSize)
                .setMaxWorkRequests(maxWorkRequests)
                .setProxyProvider(new QueuedProxyProvider(maxWorkRequests))
                .setBufferManager(new TwoSidedBufferManager(maxBufferSize, maxWorkRequests))
                .setSvcManager(new TwoSidedSVCManager(maxBufferSize, maxWorkRequests))
                .setWorkCompletionHandler(new NetworkCompletionEventHandler(packetDispatcher));
        // create endpoint
        return new ActiveRdmaCommunicator(endpointGroup, id, serverSide, dependencies);
    }
}
