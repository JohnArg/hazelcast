package discovery.client.rpc.response;

import discovery.common.api.ServerIdentifier;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Manages pending responses to RPC calls.
 */
public class PendingResponseManager {
    // Maps of <operation IDs, response futures>
    private Map<Long, CompletableFuture<Set<ServerIdentifier>>> registerServerPendingResponses;
    private Map<Long, CompletableFuture<Boolean>> unregisterServerPendingResponses;
    private Map<Long, CompletableFuture<Set<ServerIdentifier>>> getRegisteredServersPendingResponses;

    public PendingResponseManager() {
        registerServerPendingResponses = new HashMap<>();
        unregisterServerPendingResponses = new HashMap<>();
        getRegisteredServersPendingResponses = new HashMap<>();
    }

    public Map<Long, CompletableFuture<Set<ServerIdentifier>>> registerServerPendingResponses() {
        return registerServerPendingResponses;
    }

    public Map<Long, CompletableFuture<Boolean>> unregisterServerPendingResponses() {
        return unregisterServerPendingResponses;
    }

    public Map<Long, CompletableFuture<Set<ServerIdentifier>>> getRegisteredServersPendingResponses() {
        return getRegisteredServersPendingResponses;
    }
}
