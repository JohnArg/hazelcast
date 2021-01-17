package discovery.client.networking;

import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Creates an {@link jarg.rdmarpc.networking.communicators.RdmaCommunicator RdmaCommunicator} and connects
 * it to a remote service. The {@link jarg.rdmarpc.networking.communicators.RdmaCommunicator RdmaCommunicator}
 * can then be used to exchange data with the service.
 */
public class ServiceConnectionComponent {
    private static final Logger logger = LoggerFactory.getLogger(ServiceConnectionComponent.class);

    RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ActiveRdmaCommunicator rdmaCommunicator;
    private InetSocketAddress serviceAddress;
    // Rdma endpoint properties
    private int maxWorkRequests;
    private int cqSize;
    private int timeout;
    private boolean polling;
    private int maxSge;
    private int maxNetworkBufferSize;


    public ServiceConnectionComponent(InetSocketAddress serviceAddress, int maxWorkRequests,
                                      int cqSize, int timeout, boolean polling, int maxSge, int maxNetworkBufferSize) {
        this.serviceAddress = serviceAddress;
        this.maxWorkRequests = maxWorkRequests;
        this.cqSize = cqSize;
        this.timeout = timeout;
        this.polling = polling;
        this.maxSge = maxSge;
        this.maxNetworkBufferSize = maxNetworkBufferSize;
    }

    /**
     * Connects to a remote service.
     * @return true on success or false on failure.
     */
    public boolean connect(){
        // An endpoint group is needed to create RDMA endpoints
        try {
            endpointGroup = new RdmaActiveEndpointGroup<>(timeout, polling, maxWorkRequests, maxSge, cqSize);
        } catch (IOException e) {
            logger.error("Cannot create endpoint group.", e);
            return false;
        }
        // The group requires an endpoint factory to create the endpoints
        ClientCommunicatorFactory communicatorFactory = new ClientCommunicatorFactory(endpointGroup,
                maxNetworkBufferSize, maxWorkRequests);
        endpointGroup.init(communicatorFactory);
        // Get a client endpoint
        try {
            rdmaCommunicator = endpointGroup.createEndpoint();
        } catch (IOException e) {
            logger.error("Cannot create endpoint.", e);
            return false;
        }
        // Connect to remote service
        try {
            rdmaCommunicator.connect(serviceAddress, timeout);
        } catch (Exception e) {
            logger.error("Cannot connect to remote service.", e);
        }
        return true;
    }

    public void shutdown(){
        try {
            rdmaCommunicator.close();
            endpointGroup.close();
        } catch (IOException | InterruptedException e) {
           logger.error("Cannot close endpoint.", e);
        }
    }

    public void finalize(){
        shutdown();
    }

    public ActiveRdmaCommunicator getRdmaCommunicator() {
        return rdmaCommunicator;
    }
}
