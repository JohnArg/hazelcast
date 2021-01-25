package discovery.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Starts the discovery service with parameters read from a
 * configuration file.
 */
public class DiscoveryServiceStarter {
    private static final String DISCOVERY_CONFIG = "discovery.properties";
    private static final Logger logger = LoggerFactory.getLogger(DiscoveryServiceStarter.class.getSimpleName());

    public static void main(String[] args) {
        DiscoveryConfig config = new DiscoveryConfig();
        try{
            config.loadFromProperties(DISCOVERY_CONFIG);
        }catch (Exception e){
            logger.error("Cannot read configuration from properties file.", e);
            return;
        }
        InetSocketAddress listeningAddress = null;
        try{
            InetAddress listeningIp = InetAddress.getByName(config.getDiscoveryAddress());
            int port = config.getDiscoveryPort();
            listeningAddress = new InetSocketAddress(listeningIp, port);
        }catch (IOException e){
            logger.error("Cannot cannot extract listening IP and port from configuration.", e);
            return;
        }
        DiscoveryService service = new DiscoveryService(listeningAddress, config.getServerBacklog(),
                config.getMaxWRs(), config.getCqSize(), config.getTimeout(), config.isPolling(),
                config.getMaxSge(), config.getMaxBufferSize(), config.getWorkerThreadsNum());
        service.start();
    }
}
