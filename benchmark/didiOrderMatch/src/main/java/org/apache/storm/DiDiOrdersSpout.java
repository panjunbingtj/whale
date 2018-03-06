package org.apache.storm;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 * 重写KafkaSpout 增加一些性能测试Metrics
 */
public class DiDiOrdersSpout<K, V> extends KafkaSpout<K, V> {

    public DiDiOrdersSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    @Override
    public void ack(Object messageId) {
        super.ack(messageId);
    }

    @Override
    public void fail(Object messageId) {
        super.fail(messageId);
    }

    @Override
    public void close() {
        super.close();
    }
}
