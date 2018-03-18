package org.apache.storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.ByTopicRecordTranslator;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.report.ThroughputReportBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * locate org.apache.storm.starter
 * Created by mastertj on 2018/3/5.
 * NASDAQ股票交易Topology
 * storm jar NASDAQStockDeal-2.0.0-SNAPSHOT.jar org.apache.storm.StockeDealThroughputTopology StockeDealThroughputTopology stockdealTopic 30 1 60
 */
public class StockeDealThroughputTopology {
    public static final String KAFKA_SPOTU_ID ="kafka-spout";
    public static final String THROUGHPUT_BOLT_ID ="throughput-bolt";
    public static final String LATENCY_BOLT_ID ="latency-bolt";
    public static final String STACKDEAL_BOLT_ID ="stockdeal-bolt";
    public static final String SPOUT_STREAM_ID ="spout_stream";
    public static final String ACKCOUNT_STREAM_ID="ackcountstream";
    public static final String LATENCYTIME_STREAM_ID="latencytimestream";
    public static final String KAFKA_LOCAL_BROKER = "node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092";
    //public static final String KAFKA_LOCAL_BROKER = "ubuntu1:9092,ubuntu2:9092,ubuntu4:9092";

    public static void main(String[] args) throws Exception{
        String topologyName=args[0];
        String topic=args[1];
        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutInstancesNum=Integer.valueOf(args[3]);
        Integer boltInstancesNum=Integer.valueOf(args[4]);

        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout(KAFKA_SPOTU_ID, new StockeDealThroughputSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER,topic)), spoutInstancesNum);
        builder.setBolt(STACKDEAL_BOLT_ID, new StockeDealThroughputBolt(),boltInstancesNum).allGrouping(KAFKA_SPOTU_ID,SPOUT_STREAM_ID);
        builder.setBolt(THROUGHPUT_BOLT_ID, new ThroughputReportBolt(),1).shuffleGrouping(KAFKA_SPOTU_ID,ACKCOUNT_STREAM_ID);
        Config config=new Config();
        //config.setDebug(true);
        //config.setNumAckers(0);
        //config.setMessageTimeoutSecs(30);

        if(args!=null && args.length <= 0){
            Utils.sleep(50*1000);//50s
        }else {
            config.setNumWorkers(numworkers);//每一个Worker 进程默认都会对应一个Acker 线程
            StormSubmitter.submitTopology(topologyName,config,builder.createTopology());
        }
    }

    public static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,String topic) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), SPOUT_STREAM_ID);
//        trans.forTopic(TOPIC_2,
//                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
//                new Fields("topic", "partition", "offset", "key", "value"), TOPIC_2_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{topic})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "StockDealThroughputGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .build();
    }

    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(0),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
