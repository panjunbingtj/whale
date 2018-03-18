package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class StockeDealThroughputBolt extends BaseRichBolt {

    private Set<String> drivers=new HashSet<>();
    private Random random=new Random();

    private Timer timer;
    private int thisTaskId =0;
    private long tuplecount=0; //记录单位时间ACK的元组数量
    private OutputCollector outputCollector;
    private static final String ACKCOUNT_STREAM_ID="tuplecountstream";

    private void waitForTimeMills(long timeMills){
        Long startTimeMllls=System.currentTimeMillis();
        while (true){
            Long endTimeMills=System.currentTimeMillis();
            if(endTimeMills-startTimeMllls>=timeMills)
                break;
        }
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector=collector;

        thisTaskId=context.getThisTaskId();
        timer=new Timer();

        //设置计时器没1s计算时间
        timer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                //将最后结果输出到日志文件中
                outputCollector.emit(new Values(tuplecount,System.currentTimeMillis(),thisTaskId));
                tuplecount = 0;
            }
        }, 1,1000);// 设定指定的时间time,此处为1000毫秒

    }

    @Override
    public void execute(Tuple input) {
        String topic=input.getStringByField("topic");
        Integer partition=input.getIntegerByField("partition");
        Long offset=input.getLongByField("offset");
        String key=input.getStringByField("key");
        String value=input.getStringByField("value");
        String[] strs=value.split("\\s+");

        tuplecount++;
        //simulation match function
        //waitForTimeMills(2);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tuplecount","timeinfo","taskid"));
    }

}
