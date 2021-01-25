package discovery.client;

import discovery.client.networking.ClientCommunicatorDependencies;
import discovery.client.networking.ServiceConnectionComponent;
import discovery.client.rpc.DiscoveryServiceProxy;
import discovery.client.rpc.request.DiscoveryRequestIdGenerator;
import discovery.client.rpc.response.PendingResponseManager;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.rpc.request.RequestIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DiscoveryClient {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryClient.class.getSimpleName());

    private ServiceConnectionComponent serviceConnectionComponent;
    private PendingResponseManager responseManager;
    private RequestIdGenerator<Long> requestIdGenerator;
    private InetSocketAddress serviceAddress;
    // Rdma endpoint properties
    private int maxWorkRequests;
    private int cqSize;
    private int timeout;
    private boolean polling;
    private int maxSge;
    private int maxNetworkBufferSize;

    public DiscoveryClient(InetSocketAddress serviceAddress, int maxWorkRequests, int cqSize, int timeout,
                           boolean polling, int maxSge, int maxNetworkBufferSize) {
        this.serviceAddress = serviceAddress;
        this.maxWorkRequests = maxWorkRequests;
        this.cqSize = cqSize;
        this.timeout = timeout;
        this.polling = polling;
        this.maxSge = maxSge;
        this.maxNetworkBufferSize = maxNetworkBufferSize;
        serviceConnectionComponent = new ServiceConnectionComponent(serviceAddress, maxWorkRequests, cqSize,
                timeout, polling, maxSge, maxNetworkBufferSize);
    }

    public DiscoveryServiceProxy generateDiscoveryServiceProxy(){
        serviceConnectionComponent.connect();
        ActiveRdmaCommunicator communicator = serviceConnectionComponent.getRdmaCommunicator();
        ClientCommunicatorDependencies dependencies = (ClientCommunicatorDependencies)
                communicator.getDependencies();
        responseManager = dependencies.getResponseManager();
        requestIdGenerator = new DiscoveryRequestIdGenerator(0);
        return new DiscoveryServiceProxy(serviceConnectionComponent.getRdmaCommunicator(), responseManager,
                requestIdGenerator);
    }

    public InetSocketAddress getClientAddress(){
        InetSocketAddress address = null;
        try {
            address = (InetSocketAddress) serviceConnectionComponent.getRdmaCommunicator().getSrcAddr();
        } catch (IOException e) {
            logger.error("Cannot get this endpoint's InetSocket address.", e);
        }
        return address;
    }

    public void shutDown(){
        serviceConnectionComponent.shutdown();
    }
}
