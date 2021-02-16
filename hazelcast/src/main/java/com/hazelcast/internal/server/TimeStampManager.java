package com.hazelcast.internal.server;

import com.hazelcast.logging.ILogger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Contains timestamps of Raft operations. Used to calculate latencies of Raft RPCs.
 */
public class TimeStampManager {

    private List<TimeStamp> operationTimeStamps;
    private ILogger logger;

    public TimeStampManager(ILogger logger){
        operationTimeStamps = new ArrayList<>();
        this.logger = logger;
    }


    public void createTimeStamp(String operationName, String memberId, int rpcId, TimeStampCreatorType creatorType){
        operationTimeStamps.add(new TimeStamp(operationName, memberId, rpcId, creatorType));
    }

    public void export(String filename){
        try(FileWriter fileWriter = new FileWriter(filename);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            bufferedWriter.write("# Timestamps created : " + operationTimeStamps.size() + "\r\n");
            bufferedWriter.write("# <operation> <rpc id> <creator type> <time>\r\n");
            for(TimeStamp timeStamp : operationTimeStamps){
                bufferedWriter.write(timeStamp.toString() + "\r\n");
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            logger.severe("[TimeStampManager] Error in exporting to file");
        }
    }



    public enum TimeStampCreatorType{
        SENDER,
        RECEIVER
    }

    private class TimeStamp{
        String operationName;
        String memberId;
        int rpcId;
        TimeStampCreatorType creatorType;
        long time;

        public TimeStamp(String operationName, String memberId, int rpcId, TimeStampCreatorType creatorType){
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

}
