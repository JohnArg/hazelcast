package discovery.service.rpc.invocators;

import discovery.common.DiscoveryPacket;
import jarg.rdmarpc.networking.communicators.RdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;
import jarg.rdmarpc.rpc.serialization.RpcDataSerializer;
import org.slf4j.Logger;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;


public abstract class AbstractDiscoveryInvocator implements RpcOperationInvocator {

    private final Logger logger;

    public AbstractDiscoveryInvocator(Logger logger) {
        this.logger = logger;
    }

    protected void sendResponse(DiscoveryPacket requestPacket, AbstractDataSerializer responseSerializer,
                                Boolean isErrorResponse) {
        // get information from the request packet
        RdmaCommunicator endpoint = requestPacket.getWorkRequestProxy().getRdmaCommunicator();
        int invokedOperationType = requestPacket.getOperationType();
        long invokedOperationId = requestPacket.getOperationId();
        WorkRequestProxy workRequestProxy = endpoint.getWorkRequestProxyProvider()
                .getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
        // prepare the response packet
        DiscoveryPacket responsePacket = new DiscoveryPacket(workRequestProxy);
        if(isErrorResponse){
            responsePacket.setMessageType(RpcMessageType.ERROR);
        }else{
            responsePacket.setMessageType(RpcMessageType.RESPONSE);
        }
        responsePacket.setOperationType(invokedOperationType)
                .setOperationId(invokedOperationId)
                .setPacketNumber(0);
        // Prepare to serialize payload if necessary
        if((!isErrorResponse) && (responseSerializer != null)){
            responseSerializer.setWorkRequestProxy(workRequestProxy);
        }
        try {
            responsePacket.writeToWorkRequestBuffer(responseSerializer);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot serialize RPC packet.", e);
            workRequestProxy.releaseWorkRequest();
            return;
        }
        // get the buffer ready for reading
        workRequestProxy.getBuffer().flip();
        // send response
        responsePacket.getWorkRequestProxy().post();
    }
}
