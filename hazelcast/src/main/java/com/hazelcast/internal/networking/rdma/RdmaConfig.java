package com.hazelcast.internal.networking.rdma;

import java.io.*;
import java.net.InetAddress;
import java.util.Properties;

/**
 * Will contain settings for the creation of RDMA endpoints and
 * can be used by RDMA endpoint factories.
 */
public class RdmaConfig {

    /* ********************************************************
     *   Configurable Parameters
     * ********************************************************/

    /**
     * The address to bind the local server to. It should not be a loopback
     * address.
     */
    private String rdmaAddress;
    /**
     * The port that the RDMA server will listen to
     */
    private int rdmaListeningPort;
    /**
     * How many times to attempt to connect to a remote server.
     */
    private int connectionRetries;
    /**
     * How much time should the thread sleep before re-attempting
     * to connect.
     */
    private int connectionRetryDelay;
    /**
     * How much time to wait until the discovery process through non-RDMA methods
     * is complete.
     * Used when members discover each other with other methods than RDMA, before
     * starting RDMA communications.
     */
    private int discoveryTimeout;
    /**
     * How many times to ask the CP subsystem which are the CP members.
     * Used when RDMA communications are implemented only for the CP members
     * and the {@link RdmaService} has to query the
     * {@link com.hazelcast.cp.internal.RaftService RaftService}
     * which are the CP members. Because exceptions might be thrown, the process
     * will be attempted a specified number of retries.
     */
    private int cpDiscoveryRetries;
    /**
     * Endpoint timeout (DiSNI property).
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
     * Creates an empty RDMA config. The properties are not set to defaults.
     * Call {@link RdmaConfig#loadFromProperties(String)}
     * to fill the properties with the appropriate values.
     */
    public RdmaConfig(){ }

    public RdmaConfig(String rdmaAddress, int listeningPort,
                      int connectionRetries, int connectionRetryDelay,
                      int discoveryTimeout, int cpDiscoveryRetries, int timeout, boolean polling, int maxWRs,
                      int maxSge, int cqSize, int serverBacklog, int maxBufferSize) {
        this.rdmaAddress = rdmaAddress;
        this.rdmaListeningPort = listeningPort;
        this.connectionRetries = connectionRetries;
        this.connectionRetryDelay = connectionRetryDelay;
        this.discoveryTimeout = discoveryTimeout;
        this.cpDiscoveryRetries = cpDiscoveryRetries;
        this.timeout = timeout;
        this.polling = polling;
        this.maxWRs = maxWRs;
        this.maxSge = maxSge;
        this.cqSize = cqSize;
        this.serverBacklog = serverBacklog;
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Load RDMA settings from a {@link Properties} file.
     * @param filename the name of the file to read the properties from.
     * @throws Exception
     */
    public void loadFromProperties(String filename) throws Exception {
        InputStream fileInputStr = getClass().getClassLoader().getResourceAsStream(filename);
        Properties properties = new Properties();
        properties.load(fileInputStr);

        rdmaAddress = properties.getProperty("rdma.address");
        rdmaListeningPort = Integer.parseInt(properties.getProperty("rdma.listeningPort"));
        connectionRetries = Integer.parseInt(properties.getProperty("rdma.connectionRetries"));
        connectionRetryDelay = Integer.parseInt(properties.getProperty("rdma.connectionRetryDelay"));
        discoveryTimeout = Integer.parseInt(properties.getProperty("rdma.discoveryTimeout"));
        cpDiscoveryRetries = Integer.parseInt(properties.getProperty("rdma.cpDiscoveryRetries"));
        timeout = Integer.parseInt(properties.getProperty("rdma.timeout"));
        polling = Boolean.parseBoolean(properties.getProperty("rdma.polling"));
        maxWRs = Integer.parseInt(properties.getProperty("rdma.maxWRs"));
        maxSge = Integer.parseInt(properties.getProperty("rdma.maxSge"));
        cqSize = Integer.parseInt(properties.getProperty("rdma.cqSize"));
        serverBacklog = Integer.parseInt(properties.getProperty("rdma.serverBacklog"));
        maxBufferSize = Integer.parseInt(properties.getProperty("rdma.maxBufferSize"));

    }

    public String getRdmaAddress() {
        return rdmaAddress;
    }

    public int getRdmaListeningPort() {
        return rdmaListeningPort;
    }

    public int getTimeout() {
        return timeout;
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

    public int getConnectionRetries() {
        return connectionRetries;
    }

    public boolean isPolling() {
        return polling;
    }

    public int getConnectionRetryDelay() {
        return connectionRetryDelay;
    }

    public int getDiscoveryTimeout() {
        return discoveryTimeout;
    }

    public int getCpDiscoveryRetries() {
        return cpDiscoveryRetries;
    }

    @Override
    public String toString() {
        return "RdmaConfig{" +
                "rdmaAddress='" + rdmaAddress + '\'' +
                ", rdmaListeningPort=" + rdmaListeningPort +
                ", connectionRetries=" + connectionRetries +
                ", connectionRetryDelay=" + connectionRetryDelay +
                ", discoveryTimeout=" + discoveryTimeout +
                ", cpDiscoveryRetries=" + cpDiscoveryRetries +
                ", timeout=" + timeout +
                ", polling=" + polling +
                ", maxWRs=" + maxWRs +
                ", maxSge=" + maxSge +
                ", cqSize=" + cqSize +
                ", serverBacklog=" + serverBacklog +
                ", maxBufferSize=" + maxBufferSize +
                '}';
    }
}
