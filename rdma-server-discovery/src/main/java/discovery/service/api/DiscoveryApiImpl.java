package discovery.service.api;

import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;

import java.util.HashSet;
import java.util.Set;

public class DiscoveryApiImpl implements DiscoveryApi {
    private Set<ServerIdentifier> serverIdentifiers;

    public DiscoveryApiImpl(){
        serverIdentifiers = new HashSet<>();
    }

    @Override
    public synchronized Set<ServerIdentifier> registerServer(ServerIdentifier identifier) {
        serverIdentifiers.add(identifier);
        return serverIdentifiers;
    }

    @Override
    public synchronized boolean unregisterServer(ServerIdentifier identifier) {
        return serverIdentifiers.remove(identifier);
    }

    @Override
    public synchronized Set<ServerIdentifier> getRegisteredServers() {
        return serverIdentifiers;
    }
}
