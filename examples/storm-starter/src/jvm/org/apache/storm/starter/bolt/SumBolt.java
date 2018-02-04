package org.apache.storm.starter.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by 10564 on 2018-02-02.
 */
public class SumBolt extends BaseBasicBolt{
    private static final Logger LOG = LoggerFactory.getLogger(SumBolt.class);
    private static Integer sum;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        super.prepare(topoConf, context);
        sum = 0;
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        LOG.info("the time of receiving tuple in bolt: {}", System.currentTimeMillis());

//        for (Integer i :(ArrayList<Integer>) input.getValue(0)){
//            sum += i;
//        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.declare(new Fields("sum"));
    }
}
