package com.hazelcast.internal.networking.rdma;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * Interface that specifies the functionality that must be implemented by an RDMA Service.
 */
public interface RdmaService {

    /**
     * Sends an operation to a target address.
     * @param op the operation to send.
     * @param target the remote address to which the operation will be sent.
     * @return true for success or false for errors
     */
    boolean send(Operation op, Address target);

    /**
     * Update the state of the service. If there are listeners, they should be notified.
     * @param newState the new state ({@link RdmaServiceState}) of the service.
     */
    void setState(RdmaServiceState newState);

    /**
     * Get the latest state of the service.
     */
    RdmaServiceState getLatestState();

    /**
     * Check if there is an RDMA connection towards this address.
     * @param address the address to check against.
     * @return true if there is an RDMA connection, false otherwise.
     */
    boolean isConnectedWithRdma(Address address);

    /* *****************************************************************
     * Manage listeners to this service's events (Observer Pattern)
     * ****************************************************************/

    /**
     * Register a listener for events on the {@link RdmaService}
     * @return true on success, false on failure
     */
    boolean registerListener(RdmaServiceState eventType, RdmaServiceListener listener);

    /**
     * Remove a registered event listener.
     * @return true on success, false on failure
     */
    boolean removeListener(RdmaServiceState eventType, RdmaServiceListener listener);

    /**
     * Notify the registered listeners about an event. This a <i>push</i> implementation
     * of sending notifications.
     */
    void notifyListeners(RdmaServiceState eventType);

}
