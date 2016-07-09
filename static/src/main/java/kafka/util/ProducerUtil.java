package kafka.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by iuan on 2016/7/9.
 */
public class ProducerUtil {
    private static KafkaProducer<String, String> producer = null;

    private static KafkaProducer<String, String> getProducer(){
        if(producer == null) {
            Properties props = new Properties();
            props.put("metadata.broker.list", KafkaProperty.kafkaServer);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("request.required.acks", "1");
            producer = new KafkaProducer<String, String>(props);
        }
        return producer;
    }

    public static void write(String topic, String msg) {
        KafkaProducer<String, String> producer = ProducerUtil.getProducer();
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic, msg);
        producer.send(record);
    }
}
