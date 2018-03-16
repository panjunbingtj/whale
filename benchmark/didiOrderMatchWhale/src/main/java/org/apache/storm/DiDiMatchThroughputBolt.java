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
public class DiDiMatchThroughputBolt extends BaseRichBolt {

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
        String[] strs=value.split(",");
        Order order=new Order(strs[0],strs[1],strs[2]);
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

    public class Order{
        private String orderId;
        private String time;
        private String matchDriverId;

        public Order() {
        }

        public Order(String orderId, String time, String matchDriverId) {
            this.orderId = orderId;
            this.time = time;
            this.matchDriverId = matchDriverId;
        }

        public String getOrderId() {
            return orderId;
        }

        public void setOrderId(String orderId) {
            this.orderId = orderId;
        }

        public String getTime() {
            return time;
        }

        public void setTime(String time) {
            this.time = time;
        }

        public String getMatchDriverId() {
            return matchDriverId;
        }

        public void setMatchDriverId(String matchDriverId) {
            this.matchDriverId = matchDriverId;
        }

        @Override
        public String toString() {
            return "Orders{" +
                    "orderId='" + orderId + '\'' +
                    ", time='" + time + '\'' +
                    ", matchDriverId='" + matchDriverId + '\'' +
                    '}';
        }
    }
}
