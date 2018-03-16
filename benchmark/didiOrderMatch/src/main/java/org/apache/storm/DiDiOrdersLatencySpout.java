package org.apache.storm;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutMessageId;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 * 重写KafkaSpout 增加一些性能测试Metrics
 */
public class DiDiOrdersLatencySpout<K, V> extends KafkaSpout<K, V> {
    private Timer timer;
    private int thisTaskId =0;
    private SpoutOutputCollector spoutOutputCollector;
    private static final String LATENCYTIME_STREAM_ID="latencytimestream";
    private long totalDelay=0; //总和延迟 500个tuple
    private long tuplecount=0;
    private long startTimeMills;
    private Logger LOG= LoggerFactory.getLogger(DiDiOrdersLatencySpout.class);

    public DiDiOrdersLatencySpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        thisTaskId=context.getThisTaskId();
        timer=new Timer();
        this.spoutOutputCollector= collector;
        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0) {
                    double avgDelay= ((double) totalDelay / (double) tuplecount);
                    avgDelay=(double) Math.round(avgDelay*100)/100;
                    collector.emit(LATENCYTIME_STREAM_ID,new Values(thisTaskId,avgDelay,startTimeMills));
                    totalDelay=0;
                    tuplecount=0;
                }
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    @Override
    public void ack(Object messageId) {
        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
        startTimeMills=latencyHashMap.get(msgId);
        long endTime=System.currentTimeMillis();
        long latency=endTime-startTimeMills;
        tuplecount++;
        totalDelay+=latency;
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

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        super.declareOutputFields(outputFieldsDeclarer);
        outputFieldsDeclarer.declareStream(LATENCYTIME_STREAM_ID,new Fields("taskid","avgDelay","timeinfo"));
    }

}
