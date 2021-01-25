package discovery.service.api;

import discovery.common.api.DiscoveryApi;
import discovery.common.api.ServerIdentifier;
import discovery.service.DiscoveryServiceStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class DiscoveryApiImpl implements DiscoveryApi {
    private Set<ServerIdentifier> serverIdentifiers;
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryApiImpl.class.getSimpleName());

    public DiscoveryApiImpl(){
        serverIdentifiers = new HashSet<>();
    }

    @Override
    public synchronized Set<ServerIdentifier> registerServer(ServerIdentifier identifier) {
        serverIdentifiers.add(identifier);
        logger.info("Registered server : " + identifier);
        return serverIdentifiers;
    }

    @Override
    public synchronized boolean unregisterServer(ServerIdentifier identifier) {
        logger.info("Unregistering server : " + identifier);
        return serverIdentifiers.remove(identifier);
    }

    @Override
    public synchronized Set<ServerIdentifier> getRegisteredServers() {
        return serverIdentifiers;
    }
}
