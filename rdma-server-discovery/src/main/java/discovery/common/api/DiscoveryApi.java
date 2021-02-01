package discovery.common.api;

import jarg.rdmarpc.rpc.exception.RpcExecutionException;

import java.util.Set;

/**
 * The API of a registry service that will be used by RDMA-capable servers to find discover each other.
 */
public interface DiscoveryApi {

    /**
     * Registers a server with this registry service. It returns a
     * @param identifier represents the server to register.
     * @return the registered servers.
     */
    Set<ServerIdentifier> registerServer(ServerIdentifier identifier) throws RpcExecutionException;

    /**
     * Unregister a server from this registry.
     * @param identifier represents the server to register.
     * @return true on success, false otherwise.
     */
    boolean unregisterServer(ServerIdentifier identifier) throws RpcExecutionException;

    /**
     * Get all the registered servers.
     * @return the addresses of the registered servers.
     */
    Set<ServerIdentifier> getRegisteredServers() throws RpcExecutionException;

}
