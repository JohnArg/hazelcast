package com.hazelcast.internal.server.benchmarks.timestamps;

public class SerializationTimeStamp extends LowLevelTimeStamp{
    SerializationType type;

    public SerializationTimeStamp(String name, SerializationType type, long dataSize,
                                  TimeStampCreatingPoint creatingPoint) {
        super(name, dataSize, creatingPoint);
        this.type = type;
    }

    public static enum SerializationType{
        SERIALIZATION,
        DESERIALIZATION
    }

    @Override
    public String toString() {
        return name + " " + type + " " + dataSize + " " + creatingPoint + " " + time;
    }

}
