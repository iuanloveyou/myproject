package com.iuan.kafka.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

/**
 * Created by iuan on 2016/7/9.
 */
public class ConsumerUtil {

    public static KafkaConsumer<String, String> getConsumer(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperty.kafkaServer);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        return consumer;
    }
}
