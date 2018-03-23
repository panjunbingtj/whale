package org.apache.storm;

import org.apache.storm.model.CpuUsage;
import org.apache.storm.model.IoUsage;
import org.apache.storm.model.MemUsage;
import org.apache.storm.model.NetUsage;
import org.junit.Test;

/**
 * Created by 79875 on 2017/3/25.
 */
public class ResourceUsageTest {

    CpuUsage cpuUsage=CpuUsage.getInstance();
    MemUsage memUsage=MemUsage.getInstance();
    IoUsage ioUsage=IoUsage.getInstance();
    NetUsage netUsage=NetUsage.getInstance();

    private String hosts="ubuntu2";
    private String user="root";
    private String passwd="bigdata18";
    private int port=22;

    @Test
    public void testcpu() throws Exception {
        while(true){
            System.out.println(cpuUsage.get(hosts, user, passwd, 22));
            Thread.sleep(5000);
        }
    }

    @Test
    public void testmemory() throws Exception {
        while(true){
            System.out.println(memUsage.get(hosts, user, passwd, 22));
            Thread.sleep(5000);
        }
    }

    @Test
    public void testdiskio() throws Exception {
        while(true){
            System.out.println(ioUsage.get(hosts, user, passwd, 22));
            Thread.sleep(5000);
        }
    }

    @Test
    public void testNetwork() throws Exception {
        while(true){
            System.out.println(netUsage.get(hosts, user, passwd, 22));
            Thread.sleep(5000);
        }
    }
}
