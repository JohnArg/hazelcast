package com.hazelcast.internal.networking.rdma.util;

import com.hazelcast.logging.ILogger;

import java.util.Arrays;

/**
 * Minimal logger that uses an {@link ILogger}, but prefixes messages with a specific
 * String, so that they can be identified as RDMA related messages. Used to be able to
 * detect RDMA messages more easily in logs.
 */
public class RdmaLogger {
    private ILogger logger;

    public RdmaLogger(ILogger logger){
        this.logger = logger;
    }


    public void severe(Exception e){
        logger.severe("[RDMA] "+ e.getMessage() + "\nStack trace : "
                + Arrays.toString(e.getStackTrace()));
    }

    public void severe(String message){
        logger.severe("[RDMA] "+message);
    }

    public void info(String message){
        logger.info("[RDMA] "+message);
    }
}
