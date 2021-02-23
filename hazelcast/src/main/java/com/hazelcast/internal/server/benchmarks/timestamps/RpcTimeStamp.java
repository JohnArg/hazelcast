package com.hazelcast.internal.server.benchmarks.timestamps;

public class RpcTimeStamp {

    String operationName;
    String memberId;
    int rpcId;
    TimeStampCreatorType creatorType;
    long time;

    public static enum TimeStampCreatorType{
        SENDER,
        RECEIVER
    }

    public RpcTimeStamp(String operationName, String memberId, int rpcId, TimeStampCreatorType creatorType){
        this.operationName = operationName;
        this.memberId = memberId;
        this.creatorType = creatorType;
        this.rpcId = rpcId;
        time = System.nanoTime();
    }

    public long getTime() {
        return time;
    }

    public TimeStampCreatorType getCreatorType() {
        return creatorType;
    }

    @Override
    public String toString() {
        return operationName + " " + memberId + " " + rpcId + " " + creatorType + " " + time;
    }
}
