package com.hazelcast.internal.server.benchmarks.timestamps;

import com.hazelcast.logging.ILogger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains timestamps of Raft operations. Used to calculate latencies of Raft RPCs.
 */
public class TimeStampManager {

    private List<RpcTimeStamp> rpcTimeStamps;
    private List<SerializationTimeStamp> serializationTimeStamps;
    private List<LowLevelTimeStamp> lowLevelTimeStamps;
    private ILogger logger;

    public TimeStampManager(ILogger logger){
        this.rpcTimeStamps = Collections.synchronizedList(new ArrayList<>());
        this.serializationTimeStamps = Collections.synchronizedList(new ArrayList<>());
        this.lowLevelTimeStamps = Collections.synchronizedList(new ArrayList<>());
        this.logger = logger;
    }

    public enum TimeStampType{
        RPC,
        SERIALIZATION,
        LOW_LEVEL
    }

    public void createRpcTimeStamp(String operationName, String memberId, int rpcId,
                                   RpcTimeStamp.TimeStampCreatorType creatorType){
        rpcTimeStamps.add(new RpcTimeStamp(operationName, memberId, rpcId, creatorType));
    }

    public void createSerializationTimeStamp(String name,
                                             SerializationTimeStamp.SerializationType type,
                                             long dataSize,
                                             LowLevelTimeStamp.TimeStampCreatingPoint creatingPoint){
        serializationTimeStamps.add(new SerializationTimeStamp(name, type, dataSize, creatingPoint));
    }

    public void createLowLevelTimeStamp(String name, long dataSize,
                                        LowLevelTimeStamp.TimeStampCreatingPoint creatingPoint){
        lowLevelTimeStamps.add(new LowLevelTimeStamp(name, dataSize, creatingPoint));
    }


    public void export(String filename, TimeStampType type){
        try(FileWriter fileWriter = new FileWriter(filename);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            if(type.equals(TimeStampType.RPC)){
                bufferedWriter.write("# Timestamps created : " + rpcTimeStamps.size() + "\r\n");
                bufferedWriter.write("# <operation> <rpc id> <creator type> <time>\r\n");
                for(RpcTimeStamp timeStamp : rpcTimeStamps){
                    bufferedWriter.write(timeStamp.toString() + "\r\n");
                }
            }else if(type.equals(TimeStampType.SERIALIZATION)){
                bufferedWriter.write("# Timestamps created : " + serializationTimeStamps.size() + "\r\n");
                bufferedWriter.write("# <name> <type> <dataSize> <creatingPoint> <time>\r\n");
                for(SerializationTimeStamp timeStamp : serializationTimeStamps){
                    bufferedWriter.write(timeStamp.toString() + "\r\n");
                }
            }else if(type.equals(TimeStampType.LOW_LEVEL)){
                bufferedWriter.write("# Timestamps created : " + lowLevelTimeStamps.size() + "\r\n");
                bufferedWriter.write("# <name> <dataSize> <creatingPoint> <time>\r\n");
                for(LowLevelTimeStamp timeStamp : lowLevelTimeStamps){
                    bufferedWriter.write(timeStamp.toString() + "\r\n");
                }
            }
            bufferedWriter.flush();
        } catch (IOException e) {
            logger.severe("[TimeStampManager] Error in exporting to file");
        }
    }

}
