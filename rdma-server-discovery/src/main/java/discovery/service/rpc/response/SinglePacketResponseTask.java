package discovery.service.rpc.response;

import discovery.common.DiscoveryPacket;
import jarg.rdmarpc.networking.communicators.RdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxyProvider;
import jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sends an RPC response that fits in a single packet. Requires the received
 * request packet, in order to associate the response to the request.
 */
public class SinglePacketResponseTask implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(SinglePacketResponseTask.class);

    private DiscoveryPacket requestPacket;                // the packet of the request to which we'll respond
    private AbstractDataSerializer serializer;      // how to serialize the response
    private WorkRequestType sendingMethod;          // how to send the payload
    private boolean isErrorResponse;                // is it an ERROR response?

    public SinglePacketResponseTask(DiscoveryPacket requestPacket, AbstractDataSerializer serializer,
                                    WorkRequestType sendingMethod, boolean isErrorResponse) {
        this.requestPacket = requestPacket;
        this.serializer = serializer;
        this.sendingMethod = sendingMethod;
        this.isErrorResponse = isErrorResponse;
    }

    @Override
    public void run() {
        // First, get the necessary information from the request's packet
        RdmaCommunicator endpoint = requestPacket.getWorkRequestProxy().getEndpoint();
        int invokedOperationType = requestPacket.getOperationType();
        long invokedOperationId = requestPacket.getOperationId();
        // Now get a new WR from the RDMA endpoint and create a new packet for it
        WorkRequestProxyProvider proxyProvider = endpoint.getWorkRequestProxyProvider();
        WorkRequestProxy workRequestProxy = proxyProvider.getPostSendRequestBlocking(sendingMethod);
        DiscoveryPacket responsePacket = new DiscoveryPacket(workRequestProxy);
        // Prepare response packet headers
        if(isErrorResponse){
            responsePacket.setMessageType(RpcMessageType.ERROR);
        }else {
            responsePacket.setMessageType(RpcMessageType.RESPONSE);
        }
        responsePacket.setOperationType(invokedOperationType)
                .setOperationId(invokedOperationId)
                .setPacketNumber(0);
        // Prepare to serialize payload if necessary
        if((!isErrorResponse) && (serializer != null)){
            serializer.setWorkRequestProxy(workRequestProxy);
        }
        // Serialize the packet
        try {
            responsePacket.writeToWorkRequestBuffer(serializer);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot serialize RPC packet.", e);
            workRequestProxy.releaseWorkRequest();
            return;
        }
        // get the buffer ready for reading
        workRequestProxy.getBuffer().flip();
        // Time to send across the network
        endpoint.postNetOperationToNIC(workRequestProxy);
    }
}
