package org.apache.storm;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 * 重写KafkaSpout 增加一些性能测试Metrics
 */
public class DiDiOrdersThroughputSpout<K, V> extends KafkaSpout<K, V> {
    private Timer timer;
    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private SpoutOutputCollector spoutOutputCollector;
    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
    private static final String LATENCYTIME_STREAM_ID="latencytimestream";

    public DiDiOrdersThroughputSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        this.spoutOutputCollector=collector;
        thisTaskId=context.getThisTaskId();
        timer=new Timer();
        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //executor.execute(new WordCountTupleTask(new Timestamp(System.currentTimeMillis()),spoutcount));
                spoutOutputCollector.emit(ACKCOUNT_STREAM_ID,new Values(ackcount,System.currentTimeMillis(),thisTaskId));
                ackcount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {
        super.nextTuple();

    }

    @Override
    public void ack(Object messageId) {
        ackcount++;
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
        outputFieldsDeclarer.declareStream(ACKCOUNT_STREAM_ID,new Fields("tuplecount","timeinfo","taskid"));

    }

}
