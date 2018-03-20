package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class DiDiMatchBolt extends BaseRichBolt {

    private Set<String> drivers=new HashSet<>();
    private Random random=new Random();

    private OutputCollector outputCollector;
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
        //outputCollector.ack(input);
        //simulation match function
        //outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {

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
