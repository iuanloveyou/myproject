package kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by iuan on 2016/7/9.
 */
public class ProducerUtil {
    private static KafkaProducer<String, String> producer = null;

    private static KafkaProducer<String, String> getProducer(){
        if(producer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", KafkaProperty.kafkaServer);
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }

    public static void write(String topic,String key, String msg) {
        KafkaProducer<String, String> producer = ProducerUtil.getProducer();
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, msg);
        producer.send(record);
    }
}
