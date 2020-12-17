package com.hazelcast.internal.networking.rdma;

import java.io.*;
import java.util.Properties;

/**
 * Will contain settings for the creation of RDMA endpoints and
 * can be used by RDMA endpoint factories.
 */
public class RdmaConfig {

    /* ********************************************************
    *   Defaults
     * ********************************************************/

    /**
     * The port that an RDMA server will listen to.
     */
    public static final int DEFAULT_RDMA_PORT = 3000;
    /**
     * The server's backlog.
     */
    public static final int DEFAULT_SERVER_BACKLOG = 100;
    /**
     * How many times to attempt to connect to a remote server.
     */
    public static final int DEFAULT_CONNECTION_RETRIES = 10;
    /**
     * How much time should the thread sleep before re-attempting
     * to connect.
     */
    public static final int DEFAULT_CONNECTION_RETRY_DELAY = 500;

    /**
     * Endpoint timeout (DiSNI property).
     */
    public static final int DEFAULT_TIMEOUT = 1000;
    /**
     * Endpoint polling mode (DiSNI property).
     */
    public static final boolean DEFAULT_POLLING = false;
    /**
     * Endpoint max RDMA Work Requests (DiSNI property).
     */
    public static final int DEFAULT_MAX_WRS = 100;
    /**
     * Endpoint RDMA Completion Queue size (DiSNI property).
     */
    public static final int DEFAULT_CQ_SIZE = 200;
    /**
     * Endpoint Scatter/Gather elements (DiSNI property).
     */
    public static final int DEFAULT_MAX_SGE = 1;
    /**
     * Endpoint max buffer size for storing messages.
     */
    public static final int DEFAULT_MAX_BUFFER_SIZE = 200;

    /* ********************************************************
     *   Configurable Parameters
     * ********************************************************/

    private int rdmaListeningPort;
    private int connectionRetries;
    private int connectionRetryDelay;   // sleep for this amount of time
    private int timeout;
    private boolean polling;
    private int maxWRs;
    private int maxSge;
    private int cqSize;
    private int serverBacklog;
    private int maxBufferSize;

    /**
     * Creates an empty RDMA config. The properties are not set to defaults.
     * Call either {@link RdmaConfig#setDefaults()} or {@link RdmaConfig#loadFromProperties(String)}
     * to fill the properties with the appropriate values.
     */
    public RdmaConfig(){ }

    public RdmaConfig(int listeningPort, int connectionRetries, int connectionRetryDelay,
                      int timeout, boolean polling, int maxWRs,
                      int maxSge, int cqSize, int serverBacklog, int maxBufferSize) {
        this.rdmaListeningPort = listeningPort;
        this.connectionRetries = connectionRetries;
        this.connectionRetryDelay = connectionRetryDelay;
        this.timeout = timeout;
        this.polling = polling;
        this.maxWRs = maxWRs;
        this.maxSge = maxSge;
        this.cqSize = cqSize;
        this.serverBacklog = serverBacklog;
        this.maxBufferSize = maxBufferSize;
    }

    /**
     * Sets properties to default values, which are not optimal.
     */
    public void setDefaults(){
        rdmaListeningPort = DEFAULT_RDMA_PORT;
        connectionRetries = DEFAULT_CONNECTION_RETRIES;
        connectionRetryDelay = DEFAULT_CONNECTION_RETRY_DELAY;
        timeout = DEFAULT_TIMEOUT;
        polling = DEFAULT_POLLING;
        maxWRs = DEFAULT_MAX_WRS;
        maxSge = DEFAULT_MAX_SGE;
        cqSize = DEFAULT_CQ_SIZE;
        serverBacklog = DEFAULT_SERVER_BACKLOG;
        maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
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

        rdmaListeningPort = Integer.parseInt(properties.getProperty("rdma.listeningPort"));
        connectionRetries = Integer.parseInt(properties.getProperty("rdma.connectionRetries"));
        connectionRetryDelay = Integer.parseInt(properties.getProperty("rdma.connectionRetryDelay"));
        timeout = Integer.parseInt(properties.getProperty("rdma.timeout"));
        polling = Boolean.parseBoolean(properties.getProperty("rdma.polling"));
        maxWRs = Integer.parseInt(properties.getProperty("rdma.maxWRs"));
        maxSge = Integer.parseInt(properties.getProperty("rdma.maxSge"));
        cqSize = Integer.parseInt(properties.getProperty("rdma.cqSize"));
        serverBacklog = Integer.parseInt(properties.getProperty("rdma.serverBacklog"));
        maxBufferSize = Integer.parseInt(properties.getProperty("rdma.maxBufferSize"));

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

    @Override
    public String toString() {
        return "RdmaEndpointSettings{" +
                "rdmaListeningPort=" + rdmaListeningPort +
                ", connectionRetries=" + connectionRetries +
                ", connectionRetryDelay=" + connectionRetryDelay +
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
