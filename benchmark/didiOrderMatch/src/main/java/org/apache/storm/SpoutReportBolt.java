package org.apache.storm;

import org.apache.storm.model.LatencyModel;
import org.apache.storm.model.ThroughputModel;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 79875 on 2017/3/7.
 * 用来统计输出Spout输入源的吞吐量的Bolt
 */
public class SpoutReportBolt extends BaseRichBolt {
    private static Logger logger= LoggerFactory.getLogger(SpoutReportBolt.class);

    private OutputCollector outputCollector;
    private List<ThroughputModel> throughputModelList=new ArrayList<>();
    private List<LatencyModel> latencyModelList=new ArrayList<>();
    private BufferedOutputStream throughputOutput;
    private BufferedOutputStream latencyOutput;

    private static final String ACKCOUNT_STREAM_ID="ackcountstream";
    private static final String LATENCYTIME_STREAM_ID="latencytimestream";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector){
        this.outputCollector=outputCollector;
        int taskid=topologyContext.getThisTaskId();
        String throughputfileName="/storm/throughput-"+taskid;
        String latencyfileName="/storm/latency-"+taskid;

        try {
            throughputOutput=new BufferedOutputStream(new FileOutputStream(throughputfileName));
            latencyOutput=new BufferedOutputStream(new FileOutputStream(latencyfileName));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        logger.info("------------SpoutReportBolt prepare------------");
    }

    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals(ACKCOUNT_STREAM_ID)) {
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long tupplecount = tuple.getLongByField("tuplecount");
            int taskid = tuple.getIntegerByField("taskid");

            Timestamp timestamp = new Timestamp(currentTimeMills);
            throughputModelList.add(new ThroughputModel(timestamp,taskid,tupplecount));
        }else if(tuple.getSourceStreamId().equals(LATENCYTIME_STREAM_ID)){
            Long currentTimeMills = tuple.getLongByField("timeinfo");
            Long latency = tuple.getLongByField("latency");
            int taskid = tuple.getIntegerByField("taskid");

            Timestamp timestamp = new Timestamp(currentTimeMills);
            latencyModelList.add(new LatencyModel(timestamp,taskid,latency));
        }

        //将最后结果输出到日志文件中
        try {
            if(throughputModelList.size()>=10){
                for(ThroughputModel throughputModel:throughputModelList){
                    String tupplecount=String.valueOf(throughputModel.getTupplecount());
                    throughputOutput.write((tupplecount+"\n").getBytes("UTF-8"));
                }
            }

            if(latencyModelList.size()>=10){
                for(LatencyModel latencyModel:latencyModelList){
                    String latency=String.valueOf(latencyModel.getLatency());
                    Timestamp timestamp = latencyModel.getTimestamp();
                    latencyOutput.write((latency+"\t"+timestamp+"\n").getBytes("UTF-8"));
                }
            }
            throughputOutput.flush();
            latencyOutput.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        try {
            throughputOutput.flush();
            latencyOutput.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
