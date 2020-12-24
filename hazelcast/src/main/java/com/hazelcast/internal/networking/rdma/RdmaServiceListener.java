package com.hazelcast.internal.networking.rdma;

/**
 * Listens for {@link RdmaServiceState} events, triggered by an {@link RdmaService}.
 * The listener is an <i>Observer (or Subscriber)</i> of the <i>Observer Pattern</i> and must be registered
 * as such in an an {@link RdmaService} (which is the <i>Publisher (or Subject)</i>).
 */
public interface RdmaServiceListener {

    /**
     * React to an event of type {@link RdmaServiceState}. Used for <i>push</i> type notifications.
     * @param eventType the event to react to.
     */
    void update(RdmaServiceState eventType);
}
