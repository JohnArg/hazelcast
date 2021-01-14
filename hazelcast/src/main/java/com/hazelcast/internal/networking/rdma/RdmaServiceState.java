package com.hazelcast.internal.networking.rdma;

/**
 * Types of events happening in a {@link RdmaService}
 */
public enum RdmaServiceState {
    /** The service is not ready for communications yet */
    SERVICE_NOT_READY,
    /** It's not possible to establish RDMA communications */
    COMMUNICATIONS_NOT_POSSIBLE,
    /** RDMA connections have been established. RDMA communications can commence. */
    CONNECTIONS_READY,
    /** {@link RdmaService} is being shut down. */
    SERVICE_SHUTDOWN
}
