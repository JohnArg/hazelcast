package com.hazelcast.internal.networking.rdma.util;

import com.hazelcast.logging.AbstractLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LogEvent;
import com.sun.jna.platform.win32.Guid;

import java.util.Arrays;
import java.util.logging.Level;

/**
 * Decorates (wraps) an {@link ILogger} (Decorator Pattern). It prefixes messages with a specific
 * String, so that they can be identified as RDMA related messages. Used to be able to
 * detect RDMA messages more easily in logs.
 */
public class RdmaLogger implements ILogger {
    private ILogger logger;
    private final String PREFIX = "[RDMA] ";

    public RdmaLogger(ILogger logger){
        this.logger = logger;
    }

    @Override
    public void severe(String message){
        logger.severe(PREFIX+message);
    }

    @Override
    public void severe(Throwable thrown) {
        Throwable throwable = new Throwable(PREFIX + thrown.getMessage(), thrown);
        logger.severe(throwable);
    }

    @Override
    public void severe(String message, Throwable thrown) {
        logger.severe(PREFIX + message, thrown);
    }

    @Override
    public boolean isSevereEnabled() {
        return logger.isSevereEnabled();
    }

    @Override
    public void log(Level level, String message) {
        logger.log(level, PREFIX + message);
    }

    @Override
    public void log(Level level, String message, Throwable thrown) {
        logger.log(level, PREFIX + message, thrown);
    }

    @Override
    public void log(LogEvent logEvent) {
        logger.log(logEvent);
    }

    @Override
    public Level getLevel() {
        return logger.getLevel();
    }

    @Override
    public boolean isLoggable(Level level) {
        return logger.isLoggable(level);
    }

    @Override
    public void finest(String message) {
        logger.finest(PREFIX + message);
    }

    @Override
    public void finest(Throwable thrown) {
        Throwable throwable = new Throwable(PREFIX + thrown.getMessage(), thrown);
        logger.finest(throwable);
    }

    @Override
    public void finest(String message, Throwable thrown) {
        logger.finest(PREFIX + message, thrown);
    }

    @Override
    public boolean isFinestEnabled() {
        return logger.isFinestEnabled();
    }

    @Override
    public void fine(String message) {
        logger.fine(PREFIX + message);
    }

    @Override
    public void fine(Throwable thrown) {
        Throwable throwable = new Throwable(PREFIX + thrown.getMessage(), thrown);
        logger.fine(throwable);
    }

    @Override
    public void fine(String message, Throwable thrown) {
        logger.fine(PREFIX + message, thrown);
    }

    @Override
    public boolean isFineEnabled() {
        return logger.isFineEnabled();
    }

    @Override
    public void info(String message){
        logger.info(PREFIX + message);
    }

    @Override
    public void info(Throwable thrown) {
        Throwable throwable = new Throwable(PREFIX + thrown.getMessage(), thrown);
        logger.info(throwable);
    }

    @Override
    public void info(String message, Throwable thrown) {
        logger.info(PREFIX + message, thrown);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void warning(String message) {
        logger.warning(PREFIX + message);
    }

    @Override
    public void warning(Throwable thrown) {
        Throwable throwable = new Throwable(PREFIX + thrown.getMessage(), thrown);
        logger.warning(throwable);
    }

    @Override
    public void warning(String message, Throwable thrown) {
        logger.warning(PREFIX + message, thrown);
    }

    @Override
    public boolean isWarningEnabled() {
        return logger.isWarningEnabled();
    }
}
