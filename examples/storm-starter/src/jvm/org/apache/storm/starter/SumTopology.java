package org.apache.storm.starter;

import org.apache.storm.starter.bolt.SumBolt;
import org.apache.storm.starter.spout.IntegerArrayListSpout;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by 10564 on 2018-02-02.
 * 对简单数据tuple进行测试，使用ArrayList<Integer>作为数据tuple
 */
public class SumTopology extends ConfigurableTopology{
    /*
     * 启动命令：storm jar storm-starter-2.0.0-SNAPSHOT.jar org.apache.storm.starter.SumTopology sum001
     */
    public static void main(String[] args) throws Exception{
        ConfigurableTopology.start(new SumTopology(), args);
    }

    @Override
    protected int run(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new IntegerArrayListSpout(), 1);

        builder.setBolt("sum", new SumBolt(), 1).allGrouping("spout").setNumTasks(10);

        conf.setDebug(true);

        conf.setNumWorkers(2);

        String topologyName = "sum-test";

        if (args != null && args.length > 0){
            topologyName = args[0];
        }

        return submit(topologyName, conf, builder);
    }
}
