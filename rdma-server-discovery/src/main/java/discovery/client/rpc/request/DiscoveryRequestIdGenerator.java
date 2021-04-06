package discovery.client.rpc.request;

import jarg.jrcm.rpc.request.RequestIdGenerator;

/**
 * Very simple id generator that uses a long counter.
 */
public class DiscoveryRequestIdGenerator implements RequestIdGenerator<Long> {

    private long id;

    public DiscoveryRequestIdGenerator(long startingId) {
        this.id = startingId;
    }

    @Override
    public Long generateRequestId() {
        return id ++;
    }
}
