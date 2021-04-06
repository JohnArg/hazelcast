package discovery.client.networking;

import discovery.client.rpc.response.PendingResponseManager;
import discovery.common.DiscoveryCommunicatorDependencies;

public class ClientCommunicatorDependencies extends DiscoveryCommunicatorDependencies {

    private PendingResponseManager responseManager;

    public PendingResponseManager getResponseManager() {
        return responseManager;
    }

    public void setResponseManager(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
    }
}
