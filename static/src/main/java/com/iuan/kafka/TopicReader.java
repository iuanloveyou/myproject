package com.iuan.kafka;

import com.iuan.kafka.util.ConsumerUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iuan on 2016/7/9.
 */
public class TopicReader {
    private String group = "";
    private String topic = "";
    private int batch = 1;

    public TopicReader(String group, String topic, int batch) {
        this.group = group;
        this.topic = topic;
        this.batch = batch;
    }

    public void readTopic() {
        List<String> list = new ArrayList<String>();
        list.add(topic);
        KafkaConsumer<String, String> consumer = ConsumerUtil.getConsumer(group);
        consumer.subscribe(list);
        ReaderThread readerThread = new ReaderThread(consumer, 1);
        readerThread.run();
    }

    public static void main(String[] args) {
        TopicReader topicReader = new TopicReader("ssss1", "test", 1);
        topicReader.readTopic();
    }
}
