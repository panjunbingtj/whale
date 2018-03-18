package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.util.PropertiesUtil;
import org.apache.storm.util.TimeUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class StockeDealThroughputBolt extends BaseRichBolt {

    private Set<String> drivers=new HashSet<>();
    private Random random=new Random();
    private long executeTime=0;

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;
        PropertiesUtil.init("/stock.properties");
        executeTime=Long.valueOf(PropertiesUtil.getProperties("executeTimeNanos"));
    }

    @Override
    public void execute(Tuple input) {
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");
        String[] strs=value.split("\\s+");
        //outputCollector.ack(input);
        //simulation match function
        TimeUtils.waitForNanos(executeTime);
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {

    }
}
