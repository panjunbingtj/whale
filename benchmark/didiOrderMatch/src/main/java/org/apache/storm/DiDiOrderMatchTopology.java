package org.apache.storm;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

/**
 * locate org.apache.storm.starter
 * Created by mastertj on 2018/3/5.
 * DiDi滴滴打车订单匹配Topology
 * storm jar didiOrderMatch-2.0.0-SNAPSHOT.jar org.apache.storm.DiDiOrderMatchTopology DiDiOrderMatchTopology ordersTopic 30 1 60
 */
public class DiDiOrderMatchTopology {
    public static final String KAFKA_SPOTU_ID ="kafka-spout";
    public static final String DIDIMATCH_BOLT_ID ="didiMatch-bolt";
    public static final String SPOUT_STREAM_ID ="spout_stream";
    public static final String KAFKA_LOCAL_BROKER = "node101:9092,node102:9092,node103:9092,node104:9092,node105:9092,node106:9092";

    public static void main(String[] args) throws Exception{
        String topologyName=args[0];
        String topic=args[1];
        Integer numworkers=Integer.valueOf(args[2]);
        Integer spoutInstancesNum=Integer.valueOf(args[3]);
        Integer boltInstancesNum=Integer.valueOf(args[4]);

        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout(KAFKA_SPOTU_ID, new KafkaSpout<>(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER,topic)), spoutInstancesNum);
        builder.setBolt(DIDIMATCH_BOLT_ID, new DiDiMatchBolt(),boltInstancesNum).allGrouping(KAFKA_SPOTU_ID,SPOUT_STREAM_ID);

        Config config=new Config();
        config.setMessageTimeoutSecs(30);

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
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "DiDiOrderMatchGroup")
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10_000)
                .setFirstPollOffsetStrategy(EARLIEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    public static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500),
                KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2), Integer.MAX_VALUE, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10));
    }
}
