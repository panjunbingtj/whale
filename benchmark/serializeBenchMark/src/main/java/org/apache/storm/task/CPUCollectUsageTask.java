package org.apache.storm.task;

import org.apache.storm.model.CpuUsage;

import java.io.*;

/**
 * locate org.apache.storm.task
 * Created by mastertj on 2018/3/23.
 * CPU利用率收集程序
 */
public class CPUCollectUsageTask implements Runnable{
    private static CpuUsage cpuUsage=CpuUsage.getInstance();

    private BufferedWriter bufferedWriter;
    private String host="ubuntu2";
    private String user="root";
    private String passwd="bigdata18";

    public CPUCollectUsageTask(String CPUOutPutFile) {
        try {
            File file=new File(CPUOutPutFile);
            if(!file.exists()){
                file.createNewFile();
            }
            bufferedWriter=new BufferedWriter(new FileWriter(CPUOutPutFile));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        //首先休息2s等待序列化操作
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (true){
            try {
                Thread.sleep(100);
                bufferedWriter.write("CPU Usage: "+cpuUsage.get(host,user,passwd,22));
                bufferedWriter.newLine();
                bufferedWriter.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
