package org.apache.storm.starter.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

/**
 * Created by 10564 on 2018-02-02.
 */
public class IntegerArrayListSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(IntegerArrayListSpout.class);
    private static boolean flag = true;
    SpoutOutputCollector _collector;
    Random _rand;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("integerArray"));
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    @Override
    public void nextTuple() {
        if (flag){
            Utils.sleep(100);
            ArrayList<Integer> integerArray = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                integerArray.add(_rand.nextInt());
            }

            LOG.debug("Emitting tuple: {}", integerArray);

            LOG.info("the time of emitting tuple : {}", System.currentTimeMillis());
            _collector.emit(new Values(integerArray));
            flag = false;
        }
    }
}
