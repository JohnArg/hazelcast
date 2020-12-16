package com.hazelcast.internal.networking.rdma;

/**
 * Will contain settings for the creation of RDMA endpoints and
 * can be used by RDMA endpoint factories.
 */
public class RdmaEndpointSettings {
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

    public RdmaEndpointSettings(int listeningPort,  int connectionRetries, int connectionRetryDelay,
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
