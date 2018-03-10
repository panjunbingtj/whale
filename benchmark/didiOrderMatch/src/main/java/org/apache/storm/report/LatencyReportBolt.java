package org.apache.storm.report;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的延迟的Bolt
 */
public class LatencyReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(LatencyReportBolt.class);

    private OutputCollector outputCollector;
    private BufferedOutputStream latencyOutput;

    private static final String LATENCYTIME_STREAM_ID="latencytimestream";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.outputCollector=outputCollector;
        int taskid=topologyContext.getThisTaskId();
        //String latencyfileName="/storm/latency-"+taskid;
        String latencyfileName="/home/TJ/latency-"+taskid;

        try {
            latencyOutput=new BufferedOutputStream(new FileOutputStream(latencyfileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        logger.info("------------LatencyReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(LATENCYTIME_STREAM_ID)){
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long latency = tuple.getLongByField("latency");
            int taskid = tuple.getIntegerByField("taskid");

            Timestamp timestamp = new Timestamp(currentTimeMills);
            //将最后结果输出到日志文件中
            try {
                latencyOutput.write((latency+"\t"+timestamp+"\n").getBytes("UTF-8"));
                latencyOutput.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        try {
            latencyOutput.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
