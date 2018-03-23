package org.apache.storm.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * KafkaConsumer工具类 用来获取KafkaConsumer
 * 单例设计模式
 * locate org.apache.storm.util
 * Created by mastertj on 2018/3/23.
 */
public class KafkaConsumerUtils {
    private static KafkaConsumer<String, String> kafkaConsumer=null;

    private static Properties props = new Properties();

    public static KafkaConsumer getInsatnce(){
        if(null==kafkaConsumer){
            synchronized (KafkaConsumerUtils.class){
                if(null==kafkaConsumer){
                    props.put("bootstrap.servers", "ubuntu2:9092,ubuntu1:9092,ubuntu4:9092");
                    props.put("group.id", "testGroup");
                    props.put("enable.auto.commit", "true");
                    props.put("auto.offset.reset","earliest");
                    props.put("auto.commit.interval.ms", "1000");
                    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    kafkaConsumer = new KafkaConsumer<>(props);
                }
            }
        }
        return kafkaConsumer;
    }
}
