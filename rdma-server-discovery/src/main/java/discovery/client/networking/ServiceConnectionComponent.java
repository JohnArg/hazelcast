package discovery.client.networking;

import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Creates an {@link jarg.jrcm.networking.communicators.RdmaCommunicator RdmaCommunicator} and connects
 * it to a remote service. The {@link jarg.jrcm.networking.communicators.RdmaCommunicator RdmaCommunicator}
 * can then be used to exchange data with the service.
 */
public class ServiceConnectionComponent {
    private static final Logger logger = LoggerFactory.getLogger(ServiceConnectionComponent.class);

    RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ActiveRdmaCommunicator rdmaCommunicator;
    private InetSocketAddress serviceAddress;
    // jrcm endpoint properties
    private int timeout;
    private Boolean initialized;


    public ServiceConnectionComponent(InetSocketAddress serviceAddress, int maxWorkRequests,
                                      int cqSize, int timeout, boolean polling, int maxSge, int maxNetworkBufferSize) {
        this.serviceAddress = serviceAddress;
        this.timeout = timeout;
        // An endpoint group is needed to create jrcm endpoints
        try {
            endpointGroup = new RdmaActiveEndpointGroup<>(timeout, polling, maxWorkRequests, maxSge, cqSize);
            // The group requires an endpoint factory to create the endpoints
            ClientCommunicatorFactory communicatorFactory = new ClientCommunicatorFactory(endpointGroup,
                    maxNetworkBufferSize, maxWorkRequests);
            endpointGroup.init(communicatorFactory);
            initialized = true;
        } catch (IOException e) {
            logger.error("Cannot create endpoint group.", e);
            initialized = false;
        }
    }

    /**
     * Connects to a remote service.
     * @return true on success or false on failure.
     */
    public boolean connect(){
        if(!initialized){
            logger.error("Initialization failed. Connections not possible.");
            return false;
        }
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

    public void disconnect(){
        try {
            rdmaCommunicator.close();
        } catch (IOException | InterruptedException e) {
            logger.error("Cannot close endpoint.", e);
        }
    }

    public void shutdown(){
        try {
            rdmaCommunicator.close();
        } catch (IOException | InterruptedException e) {
           logger.error("Cannot close endpoint.", e);
        }

        try{
            endpointGroup.close();
        } catch (InterruptedException | IOException e) {
            logger.warn("Cannot close endpoint group.");
        }
    }

    public ActiveRdmaCommunicator getRdmaCommunicator() {
        return rdmaCommunicator;
    }
}
