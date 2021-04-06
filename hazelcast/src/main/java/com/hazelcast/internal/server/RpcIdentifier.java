package com.hazelcast.internal.server;

/**
 * Holds an RPC id, in order to match RPC requests
 * with their responses.
 */
public class RpcIdentifier {

    // Added for latency benchmarks
    public int rpcId;
}
