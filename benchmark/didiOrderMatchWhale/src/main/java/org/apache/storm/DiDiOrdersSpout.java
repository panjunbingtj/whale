package org.apache.storm;

import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Map;
import java.util.Timer;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 * 重写KafkaSpout 增加一些性能测试Metrics
 */
public class DiDiOrdersSpout<K, V> extends KafkaSpout<K, V> {
    private Timer timer;
    private int thisTaskId =0;
    private long ackcount=0; //记录单位时间ACK的元组数量
    private SpoutOutputCollector spoutOutputCollector;

    private BufferedOutputStream throughputOutput;

    private static final String ACKCOUNT_STREAM_ID="ackcountstream";

    public DiDiOrdersSpout(KafkaSpoutConfig<K, V> kafkaSpoutConfig) {
        super(kafkaSpoutConfig);
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);

        this.spoutOutputCollector=collector;
        thisTaskId=context.getThisTaskId();
        timer=new Timer();
        int taskid=context.getThisTaskId();
        String throughputfileName="/home/TJ/throughput-"+taskid;

        try {
            throughputOutput=new BufferedOutputStream(new FileOutputStream(throughputfileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //设置计时器没1s计算时间
//        timer.scheduleAtFixedRate(new TimerTask() {
//            public void run() {
//                //将最后结果输出到日志文件中
//                try {
//                    throughputOutput.write((ackcount+"\t"+new Timestamp(System.currentTimeMillis())+"\n").getBytes("UTF-8"));
//                    throughputOutput.flush();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                ackcount = 0;
//            }
//        }, 1,1000);// 设定指定的时间time,此处为1000毫秒
    }

    @Override
    public void nextTuple() {
        super.nextTuple();
    }

    @Override
    public void ack(Object messageId) {
        ackcount++;
//        final KafkaSpoutMessageId msgId = (KafkaSpoutMessageId) messageId;
//        long staryTime=latencyHashMap.get(msgId);
//        long endTime=System.currentTimeMillis();
//        long latency=endTime-staryTime;
//        spoutOutputCollector.emit(LATENCYTIME_STREAM_ID,new Values(latency,endTime,thisTaskId));
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
    }

}
