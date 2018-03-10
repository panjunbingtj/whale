//package org.apache.storm.report;
//
//import org.apache.storm.task.OutputCollector;
//import org.apache.storm.task.TopologyContext;
//import org.apache.storm.topology.OutputFieldsDeclarer;
//import org.apache.storm.topology.base.BaseRichBolt;
//import org.apache.storm.tuple.Tuple;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.BufferedOutputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.sql.Timestamp;
//import java.util.Map;
//
///**
// * Created by 79875 on 2017/3/7.
// * 用来统计输出Spout输入源的吞吐量的Bolt
// */
//public class ThroughputReportBolt extends BaseRichBolt {
//    private static Logger logger= LoggerFactory.getLogger(ThroughputReportBolt.class);
//
//    private OutputCollector outputCollector;
//    private BufferedOutputStream throughputOutput;
//
//    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
//
//    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
//        this.outputCollector=outputCollector;
//        int taskid=topologyContext.getThisTaskId();
//        //String throughputfileName="/storm/throughput-"+taskid;
//        String throughputfileName="/home/TJ/throughput-"+taskid;
//
//        try {
//            throughputOutput=new BufferedOutputStream(new FileOutputStream(throughputfileName));
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        logger.info("------------ThroughputReportBolt prepare------------");
//    }
//
//    public void execute(Tuple tuple) {
//        if(tuple.getSourceStreamId().equals(ACKCOUNT_STREAM_ID)) {
//            Long currentTimeMills = tuple.getLongByField("timeinfo");
//            Long tupplecount = tuple.getLongByField("tuplecount");
//            int taskid = tuple.getIntegerByField("taskid");
//
//            Timestamp timestamp = new Timestamp(currentTimeMills);
//
//            //将最后结果输出到日志文件中
//            try {
//                throughputOutput.write((tupplecount+"\t"+timestamp+"\n").getBytes("UTF-8"));
//                throughputOutput.flush();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//
//    }
//
//    @Override
//    public void cleanup() {
//        try {
//            throughputOutput.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}
