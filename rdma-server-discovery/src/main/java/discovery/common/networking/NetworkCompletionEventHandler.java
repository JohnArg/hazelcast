package discovery.common.networking;

import com.ibm.disni.verbs.IbvWC;
import discovery.common.DiscoveryPacket;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.networking.dependencies.netrequests.types.PostedRequestType;
import jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.jrcm.rpc.packets.PacketDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Handles completion events of networking requests to the NIC. Such events
 * can be triggered for example when a message is received, when a message has
 * been sent or when an error occurred, etc.
 */
public class NetworkCompletionEventHandler extends AbstractWorkCompletionHandler {
    private static final Logger logger = LoggerFactory.getLogger(NetworkCompletionEventHandler.class);

    private PacketDispatcher<DiscoveryPacket> packetPacketDispatcher;

    public NetworkCompletionEventHandler(PacketDispatcher<DiscoveryPacket> packetPacketDispatcher) {
        this.packetPacketDispatcher = packetPacketDispatcher;
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = proxyProvider.getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for an event other than a RECEIVE, free the request
        if(!workRequestProxy.getWorkRequestType().equals(WorkRequestType.TWO_SIDED_RECV)){
            workRequestProxy.releaseWorkRequest();
        }else{
            // create a packet from the Work Request
            DiscoveryPacket packet = new DiscoveryPacket(workRequestProxy);
            // send packet to packet dispatcher
            packetPacketDispatcher.dispatchPacket(packet);
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = proxyProvider.getWorkRequestProxyForWc(workCompletionEvent);
        ActiveRdmaCommunicator rdmaCommunicator = (ActiveRdmaCommunicator) workRequestProxy.getRdmaCommunicator();
        /*
         *   Print errors when the communications are ongoing. When they are shut down, we expect
         *   IBV_WC_WR_FLUSH_ERR errors to occur, since we have pre-posted RECVs to the NIC that will be
         *   flushed, which is OK. Any other errors events occurring after shutting down the communications
         *   will also be ignored.
         */
        if(rdmaCommunicator.isShutDown()){
            return;
        }

        if((workCompletionEvent.getStatus() != IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()) ||
                !(workRequestProxy.getPostType().equals(PostedRequestType.RECEIVE))){
            // prepare the message to sent
            InetSocketAddress endpointAddress = null;
            try {
                endpointAddress = (InetSocketAddress) rdmaCommunicator.getDstAddr();
            } catch (IOException e) {
                //ignore
            }
            StringBuilder messageBuilder = new StringBuilder("Error : {");
            messageBuilder.append("WR id : "+ workCompletionEvent.getWr_id()+", ");
            messageBuilder.append("WC status : " + workCompletionEvent.getStatus() + ", ");
            messageBuilder.append("WC opcode : " + workCompletionEvent.getOpcode() + ", ");
            messageBuilder.append("WR type : ");
            // following might be null
            messageBuilder.append(workRequestProxy.getWorkRequestType());
            if(endpointAddress != null){
                messageBuilder.append(", address : " + endpointAddress +"}");
            }else{
                messageBuilder.append("}");
            }
            logger.error(messageBuilder.toString());
        }
        // must free the request if the communicator is not shut down
        workRequestProxy.releaseWorkRequest();
    }

    public PacketDispatcher<DiscoveryPacket> getPacketPacketDispatcher() {
        return packetPacketDispatcher;
    }
}
