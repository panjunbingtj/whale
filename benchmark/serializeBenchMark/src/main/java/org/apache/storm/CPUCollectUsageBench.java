package org.apache.storm;

import org.apache.storm.task.CPUCollectUsageTask;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/23.
 * java -cp serializeBenchMark-2.0.0-SNAPSHOT.jar org.apache.storm.CPUCollectUsageBench
 */
public class CPUCollectUsageBench {
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    public static void main(String[] args) throws InterruptedException, IOException {
        executor.execute(new CPUCollectUsageTask("/home/TJ/CpuUsage.txt"));
        //executor.execute(new CPUCollectUsageTask("benchmark/serializeBenchMark/data/CpuUsage.txt"));
        executor.awaitTermination(1000*60*10, TimeUnit.MILLISECONDS);
    }
}
