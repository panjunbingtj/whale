package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.util.PropertiesUtil;

import java.util.*;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class StockeDealLatencyBolt extends BaseRichBolt {

    private Set<String> drivers=new HashSet<>();
    private Random random=new Random();

    private int thisTaskId =0;
    private Timer timer;
    private int tuplecount=0;
    private long totalDelay=0; //总和延迟 500个tuple
    private long startTimeMills;
    private OutputCollector outputCollector;
    private static final String ACKCOUNT_STREAM_ID="tuplecountstream";

    private long executeTime=0;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;

        thisTaskId=context.getThisTaskId();
        PropertiesUtil.init("/stock.properties");
        executeTime=Long.valueOf(PropertiesUtil.getProperties("executeTimeNanos"));

        timer=new Timer();
        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                if(startTimeMills!=0){
                    double avgDelay= ((double) totalDelay / (double) tuplecount);
                    avgDelay=(double) Math.round(avgDelay*100)/100;
                    collector.emit(new Values(thisTaskId,avgDelay,startTimeMills));
                    totalDelay=0;
                    tuplecount=0;
                }
            }
        }, 1,10000);// 设定指定的时间time,此处为10000毫秒

    }

    @Override
    public void execute(Tuple input) {
        startTimeMills=input.getLongByField("timeinfo");
        Long delay=System.currentTimeMillis()-startTimeMills;
        if(delay<0)
            return;
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");
        String[] strs=value.split("\\s+");

        tuplecount++;
        totalDelay+=delay;

        //simulation match function
        //waitForTimeMills(2);
        //TimeUtils.waitForTimeMills(executeTime);
        //outputCollector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskid","avgDelay","timeinfo"));
    }

}
