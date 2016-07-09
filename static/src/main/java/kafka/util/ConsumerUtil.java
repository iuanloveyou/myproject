package kafka.util;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by iuan on 2016/7/9.
 */
public class ConsumerUtil {
    private static ConsumerConnector conn = null;

    private static synchronized ConsumerConnector getConnector() {
        if(conn == null) {
            conn = Consumer.createJavaConsumerConnector(ConsumerUtil.createConsumerConfig());
        }
        return  conn;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperty.zkConnect);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static List<KafkaStream<String, String>> getStream(String groupId, String topic, int threadnum){
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
        Map<String,Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, threadnum);
        Map<String, List<KafkaStream<String, String>>> msgStream = ConsumerUtil.getConnector().createMessageStreams(topicMap, keyDecoder, valueDecoder);
        return msgStream.get(topic);
    }
}
