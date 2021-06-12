package com.hazelcast.internal.networking.rdma.util;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class LatencyKeeper {

    public LongArrayList startLatencies;
    public LongArrayList endLatencies;

    public LatencyKeeper(){
        startLatencies = new LongArrayList(2000000);
        endLatencies = new LongArrayList(2000000);
    }

    public void exportData(String filename){
        try(FileWriter fileWriter = new FileWriter(filename);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
            bufferedWriter.write("# Start latencies : " + startLatencies.size() + "\n");
            bufferedWriter.write("# End latencies : " + endLatencies.size() + "\n");
            for(int i=0; i < startLatencies.size(); i++){
                long latency = endLatencies.getLong(i) - startLatencies.getLong(i);
                bufferedWriter.write(String.valueOf(latency) + "\n");
                bufferedWriter.flush();
            }

        } catch (IOException e) {
            System.err.println("[LatencyKeeper] Error in exporting to file");
        }
    }
}
