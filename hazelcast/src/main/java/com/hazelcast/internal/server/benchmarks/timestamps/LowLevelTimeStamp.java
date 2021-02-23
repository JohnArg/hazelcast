package com.hazelcast.internal.server.benchmarks.timestamps;

public class LowLevelTimeStamp {
    String name;
    long time;
    long dataSize;
    TimeStampCreatingPoint creatingPoint;

    /**
     * Was the timestamp created at the start or at the end
     * of the serialization process?
     */
    public static enum TimeStampCreatingPoint{
        START,
        END
    }

    public LowLevelTimeStamp(String name, long dataSize, TimeStampCreatingPoint creatingPoint) {
        this.name = name;
        this.dataSize = dataSize;
        this.creatingPoint = creatingPoint;
        this.time = System.nanoTime();
    }

    public long getTime() {
        return time;
    }

    @Override
    public String toString() {
        return name + " " + dataSize + " " + creatingPoint + " " + time;
    }
}
