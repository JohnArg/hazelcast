package discovery.service;

import java.io.InputStream;
import java.util.Properties;

/**
 * Contains configuration parameters for the
 * discovery service.
 */
public class DiscoveryConfig {

    /**
     * IP address of the RDMA discovery service.
     */
    private String discoveryAddress;
    /**
     * Port of the RDMA discovery service.
     */
    private int discoveryPort;
    /**
     * Endpoint connection timeout (DiSNI property).
     */
    private int timeout;
    /**
     * Endpoint polling mode (DiSNI property).
     */
    private boolean polling;
    /**
     * Endpoint max RDMA Work Requests (DiSNI property).
     */
    private int maxWRs;
    /**
     * Endpoint Scatter/Gather elements (DiSNI property).
     */
    private int maxSge;
    /**
     * Endpoint RDMA Completion Queue size (DiSNI property).
     */
    private int cqSize;
    /**
     * The server's backlog.
     */
    private int serverBacklog;
    /**
     * Endpoint max buffer size for storing messages.
     */
    private int maxBufferSize;
    /**
     * Number of worker threads that the service will use.
     */
    private int workerThreadsNum;

    public DiscoveryConfig(){};

    public DiscoveryConfig(String discoveryAddress, int discoveryPort, int timeout, boolean polling, int maxWRs, int maxSge, int cqSize, int serverBacklog, int maxBufferSize, int workerThreadsNum) {
        this.discoveryAddress = discoveryAddress;
        this.discoveryPort = discoveryPort;
        this.timeout = timeout;
        this.polling = polling;
        this.maxWRs = maxWRs;
        this.maxSge = maxSge;
        this.cqSize = cqSize;
        this.serverBacklog = serverBacklog;
        this.maxBufferSize = maxBufferSize;
        this.workerThreadsNum = workerThreadsNum;
    }

    /**
     * Load settings from a {@link Properties} file.
     * @param filename the name of the file to read the properties from.
     * @throws Exception
     */
    public void loadFromProperties(String filename) throws Exception {
        InputStream fileInputStr = getClass().getClassLoader().getResourceAsStream(filename);
        Properties properties = new Properties();
        properties.load(fileInputStr);

        discoveryAddress = properties.getProperty("discovery.address");
        discoveryPort = Integer.parseInt(properties.getProperty("discovery.port"));
        workerThreadsNum = Integer.parseInt(properties.getProperty("discovery.workerThreadsNum"));
        timeout = Integer.parseInt(properties.getProperty("rdma.timeout"));
        polling = Boolean.parseBoolean(properties.getProperty("rdma.polling"));
        maxWRs = Integer.parseInt(properties.getProperty("rdma.maxWRs"));
        maxSge = Integer.parseInt(properties.getProperty("rdma.maxSge"));
        cqSize = Integer.parseInt(properties.getProperty("rdma.cqSize"));
        serverBacklog = Integer.parseInt(properties.getProperty("rdma.serverBacklog"));
        maxBufferSize = Integer.parseInt(properties.getProperty("rdma.maxBufferSize"));
    }

    public String getDiscoveryAddress() {
        return discoveryAddress;
    }

    public int getDiscoveryPort() {
        return discoveryPort;
    }

    public int getTimeout() {
        return timeout;
    }

    public boolean isPolling() {
        return polling;
    }

    public int getMaxWRs() {
        return maxWRs;
    }

    public int getMaxSge() {
        return maxSge;
    }

    public int getCqSize() {
        return cqSize;
    }

    public int getServerBacklog() {
        return serverBacklog;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getWorkerThreadsNum() {
        return workerThreadsNum;
    }
}
