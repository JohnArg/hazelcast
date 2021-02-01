package discovery.common.networking;

import com.ibm.disni.RdmaActiveEndpoint;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvWC;
import discovery.common.DiscoveryPacket;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkRequestProxyProvider;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;
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

    public NetworkCompletionEventHandler(AbstractWorkRequestProxyProvider proxyProvider,
                                         PacketDispatcher<DiscoveryPacket> packetPacketDispatcher) {
        super(proxyProvider);
        this.packetPacketDispatcher = packetPacketDispatcher;
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
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
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // must free the request
        workRequestProxy.releaseWorkRequest();
        // prepare the message to sent
        RdmaActiveEndpoint endpoint = (RdmaActiveEndpoint) workRequestProxy.getEndpoint();
        InetSocketAddress endpointAddress = null;
        try {
            endpointAddress = (InetSocketAddress) endpoint.getDstAddr();
        } catch (IOException e) {
            //ignore
        }
        StringBuilder messageBuilder = new StringBuilder("Error : {");
        messageBuilder.append("wr id : "+ workCompletionEvent.getWr_id()+", ");
        messageBuilder.append("status : " + workCompletionEvent.getStatus() + ", ");
        messageBuilder.append("type : " + workRequestProxy.getWorkRequestType().toString());
        if(endpointAddress != null){
            messageBuilder.append(", address : " + endpointAddress +"}");
        }else{
            messageBuilder.append("}");
        }
        // Status 5 can happen on remote side disconnect, since we have already posted
        // RECV requests for that remote side.
        if((workCompletionEvent.getStatus() != IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()) ||
                (workRequestProxy.getWorkRequestType().equals(WorkRequestType.TWO_SIDED_SEND_SIGNALED)) ){
            logger.error(messageBuilder.toString());
        }
    }

    public PacketDispatcher<DiscoveryPacket> getPacketPacketDispatcher() {
        return packetPacketDispatcher;
    }
}
