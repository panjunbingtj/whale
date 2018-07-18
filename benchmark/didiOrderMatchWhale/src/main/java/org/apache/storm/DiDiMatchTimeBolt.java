package org.apache.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/6.
 */
public class DiDiMatchTimeBolt extends BaseRichBolt {
    public static final String LATENCYTIME_STREAM_ID="latencystream";

    private Set<String> drivers=new HashSet<>();
    private Random random=new Random();

    private OutputCollector outputCollector;

    private int taskid;

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
        this.taskid=context.getThisTaskId();
    }

    @Override
    public void execute(Tuple tuple) {
        Long startTime=tuple.getLongByField("startTime");

        Long startSerializingTime=tuple.getStartSerializingTime();
        Long endSerializingTime=tuple.getEndSerializingTime();
        Long startDeserializingTime=tuple.getStartDeserializingTime();
        Long endDeserializingTime=tuple.getEndDeserializingTime();
        Long clientTime=tuple.getClientTime();
        Long serverTime=tuple.getServerTime();
        long communicationTime = tuple.getCommunicationTime();

        //logger.info(startTime+"\t"+startSerializingTime+"\t"+endSerializingTime+"\t"+startDeserializingTime+"\t"+endDeserializingTime);

        Long sendWaitTime=startDeserializingTime-startTime-communicationTime;
        Long receiveWaitTime=System.currentTimeMillis()-endDeserializingTime;
        Long deSerializingTime=endDeserializingTime-startDeserializingTime;
        Long serializingTime=endSerializingTime-startSerializingTime;

        Long beforeTimeNano=System.nanoTime()/1000;

        String topic=tuple.getStringByField("topic");
        Integer partition=tuple.getIntegerByField("partition");
        Long offset=tuple.getLongByField("offset");
        String key=tuple.getStringByField("key");
        String value=tuple.getStringByField("value");
        String[] strs=value.split(",");
        Order order=new Order(strs[0],strs[1],strs[2]);
        //outputCollector.ack(input);
        //simulation match function
        //outputCollector.ack(input);

        Long endTimeNano=System.nanoTime()/1000;
        Long computeTime=endTimeNano-beforeTimeNano;

        Long totalTime=System.currentTimeMillis()-startTime;

        //如果通信时间大于0
        if(communicationTime>0)
            this.outputCollector.emit(new Values(taskid,startTime,sendWaitTime,serializingTime,communicationTime,deSerializingTime,receiveWaitTime,computeTime,totalTime));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("taskid","startTime","sendWaitTime","serializingTime","communicationTime","deSerializingTime","receiveWaitTime","computeTime","totalTime"));
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
