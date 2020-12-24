package com.hazelcast.internal.server;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;

/**
 * Specifies the minimal functionality that a connection manager should implement.
 * It is meant to be extended with more functionality by
 * different types of connection manager interfaces.
 */
public interface MinimalServerConnectionManager {

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    default boolean transmit(Packet packet, Address target) {
        return transmit(packet, target, 0);
    }

    /**
     * Transmits a packet to a certain address.
     * <p>
     * If the connection to the target doesn't exist yet, the system will try to make the connection. In this case
     * true can be returned, even though the connection eventually can't be established.
     *
     * @param packet The Packet to transmit.
     * @param target The address of the target machine where the Packet should be transmitted.
     * @param streamId the stream id
     * @return true if the transmit was a success, false if a failure.
     * @throws NullPointerException if packet or target is null.
     */
    boolean transmit(Packet packet, Address target, int streamId);
}
