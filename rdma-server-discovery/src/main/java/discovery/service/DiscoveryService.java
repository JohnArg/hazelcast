package discovery.service;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import discovery.service.networking.ServiceCommunicatorFactory;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Server component that accepts RDMA connections from clients of the
 * Discovery Service.
 */
public class DiscoveryService {
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryService.class.getSimpleName());

    RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private List<ActiveRdmaCommunicator> inboundConnections;
    // server listening settings
    private InetSocketAddress listeningAddress;
    private int backlog;
    // Rdma endpoint properties
    RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private int maxWorkRequests;
    private int cqSize;
    private int timeout;
    private boolean polling;
    private int maxSge;
    private int maxNetworkBufferSize;


    public DiscoveryService(InetSocketAddress listeningAddress, int backlog,
                            int maxWorkRequests, int cqSize, int timeout, boolean polling,
                            int maxSge, int maxNetworkBufferSize) {
        this.listeningAddress = listeningAddress;
        this.backlog = backlog;
        this.maxWorkRequests = maxWorkRequests;
        this.cqSize = cqSize;
        this.timeout = timeout;
        this.polling = polling;
        this.maxSge = maxSge;
        this.maxNetworkBufferSize = maxNetworkBufferSize;
        inboundConnections = new ArrayList<>();
    }

    public void start(){
        // An endpoint group is needed to create RDMA endpoints
        try {
            endpointGroup = new RdmaActiveEndpointGroup<>(timeout, polling, maxWorkRequests, maxSge, cqSize);
        } catch (IOException e) {
            logger.error("Cannot create endpoint group.", e);
            return;
        }
        // The group requires an endpoint factory to create the endpoints
        ServiceCommunicatorFactory communicatorFactory = new ServiceCommunicatorFactory(
                endpointGroup, maxNetworkBufferSize, maxWorkRequests);
        endpointGroup.init(communicatorFactory);
        // Get a server endpoint
        try {
            serverEndpoint = endpointGroup.createServerEndpoint();
        } catch (IOException e) {
            logger.error("Cannot create server endpoint.", e);
            return;
        }
        // bind the server endpoint to a provided ip and port
        try {
            serverEndpoint.bind(listeningAddress, backlog);
            logger.info("Service started at address " + listeningAddress);
        } catch (Exception e) {
            logger.error("Cannot bind server to address : " + listeningAddress.toString());
            return;
        }
        // start accepting connections ------------------------------
        while(true){
            ActiveRdmaCommunicator clientEndpoint = null;
            try {
                clientEndpoint = serverEndpoint.accept();
            } catch (IOException e) {
                logger.warn("Server stopped accepting connections.", e);
                return;
            }
            inboundConnections.add(clientEndpoint);
        }
    }

    public void shutdown(){
        try {
            for (ActiveRdmaCommunicator clientEndpoint : inboundConnections) {
                clientEndpoint.close();
            }
            serverEndpoint.close();
            endpointGroup.close();
            logger.info("Shutting down server.");
        } catch (IOException | InterruptedException e) {
            logger.error("Error in closing server endpoint.", e);
        }
    }

    public List<ActiveRdmaCommunicator> getInboundConnections() {
        return inboundConnections;
    }
}
