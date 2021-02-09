package com.hazelcast.internal.networking.rdma;

import java.io.InputStream;
import java.util.Properties;

/**
 * Will contain settings for the creation of RDMA endpoints and
 * can be used by RDMA endpoint factories.
 */
public class RdmaConfig {

    /* ********************************************************
     *   Configuration Parameters
     * ********************************************************/

    /**
     * Whether to enable RDMA or not
     */
    private boolean rdmaEnable;

    /**
     * The address to bind the local server to. It should not be a loopback
     * address.
     */
    private String rdmaAddress;
    /**
     * The minimum port that the RDMA server will listen to
     */
    private int rdmaListeningPortMin;
    /**
     * The maximum port that the RDMA server will listen to,
     * if it fails to listen to the other ports between this
     * value and the minimum port value.
     */
    private int rdmaListeningPortMax;
    /**
     * How many times to retry to connect.
     */
    private int connectionRetries;
    /**
     * How much time should the thread sleep before re-attempting
     * to connect.
     */
    private int connectionRetryDelay;
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
     * Creates an empty RDMA config. The properties are not set to defaults.
     * Call {@link RdmaConfig#loadFromProperties(String)}
     * to fill the properties with the appropriate values.
     */
    public RdmaConfig(){ }


    public RdmaConfig(boolean rdmaEnable, String rdmaAddress, int rdmaListeningPortMin,
                      int rdmaListeningPortMax, int connectionRetries, int connectionRetryDelay,
                      String discoveryAddress, int discoveryPort, int timeout, boolean polling,
                      int maxWRs, int maxSge, int cqSize, int serverBacklog, int maxBufferSize) {
        this.rdmaEnable = rdmaEnable;
        this.rdmaAddress = rdmaAddress;
        this.rdmaListeningPortMin = rdmaListeningPortMin;
        this.rdmaListeningPortMax = rdmaListeningPortMax;
        this.connectionRetries = connectionRetries;
        this.connectionRetryDelay = connectionRetryDelay;
        this.discoveryAddress = discoveryAddress;
        this.discoveryPort = discoveryPort;
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

        rdmaEnable = Boolean.parseBoolean(properties.getProperty("rdma.enable"));
        if(!rdmaEnable){
            return;
        }
        rdmaAddress = properties.getProperty("rdma.address");
        rdmaListeningPortMin = Integer.parseInt(properties.getProperty("rdma.listeningPortMin"));
        rdmaListeningPortMax = Integer.parseInt(properties.getProperty("rdma.listeningPortMax"));
        connectionRetries = Integer.parseInt(properties.getProperty("rdma.connectionRetries"));
        connectionRetryDelay = Integer.parseInt(properties.getProperty("rdma.connectionRetryDelay"));
        discoveryAddress = properties.getProperty("rdma.discovery.address");
        discoveryPort = Integer.parseInt(properties.getProperty("rdma.discovery.port"));
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

    public boolean isRdmaEnabled() {
        return rdmaEnable;
    }

    public int getRdmaListeningPortMin() {
        return rdmaListeningPortMin;
    }

    public int getRdmaListeningPortMax() {
        return rdmaListeningPortMax;
    }

    public String getDiscoveryAddress() {
        return discoveryAddress;
    }

    public int getDiscoveryPort() {
        return discoveryPort;
    }

    public void setRdmaEnable(boolean rdmaEnable) {
        this.rdmaEnable = rdmaEnable;
    }

    public void setRdmaAddress(String rdmaAddress) {
        this.rdmaAddress = rdmaAddress;
    }

    public void setRdmaListeningPortMin(int rdmaListeningPortMin) {
        this.rdmaListeningPortMin = rdmaListeningPortMin;
    }

    public void setRdmaListeningPortMax(int rdmaListeningPortMax) {
        this.rdmaListeningPortMax = rdmaListeningPortMax;
    }

    public void setConnectionRetries(int connectionRetries) {
        this.connectionRetries = connectionRetries;
    }

    public void setConnectionRetryDelay(int connectionRetryDelay) {
        this.connectionRetryDelay = connectionRetryDelay;
    }

    public void setDiscoveryAddress(String discoveryAddress) {
        this.discoveryAddress = discoveryAddress;
    }

    public void setDiscoveryPort(int discoveryPort) {
        this.discoveryPort = discoveryPort;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public void setPolling(boolean polling) {
        this.polling = polling;
    }

    public void setMaxWRs(int maxWRs) {
        this.maxWRs = maxWRs;
    }

    public void setMaxSge(int maxSge) {
        this.maxSge = maxSge;
    }

    public void setCqSize(int cqSize) {
        this.cqSize = cqSize;
    }

    public void setServerBacklog(int serverBacklog) {
        this.serverBacklog = serverBacklog;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    public String toString() {
        return "RdmaConfig{" +
                "rdmaEnable=" + rdmaEnable +
                ", rdmaAddress='" + rdmaAddress + '\'' +
                ", rdmaListeningPortMin=" + rdmaListeningPortMin +
                ", rdmaListeningPortMax=" + rdmaListeningPortMax +
                ", connectionRetries=" + connectionRetries +
                ", connectionRetryDelay=" + connectionRetryDelay +
                ", discoveryAddress='" + discoveryAddress + '\'' +
                ", discoveryPort=" + discoveryPort +
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
