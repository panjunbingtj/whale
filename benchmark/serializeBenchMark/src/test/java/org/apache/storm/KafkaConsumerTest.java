package org.apache.storm;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/23.
 */
public class KafkaConsumerTest {

    @Test
    public void test(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "ubuntu2:9092,ubuntu1:9092,ubuntu4:9092");
        props.put("group.id", "testGroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset","earliest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("ordersTopic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
