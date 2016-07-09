package kafka;

import kafka.consumer.KafkaStream;
import kafka.util.ConsumerUtil;
import kafka.util.ReaderThread;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by iuan on 2016/7/9.
 */
public class TopicReader {
    private int threadNum = 10;
    private String group = "";
    private String topic = "";

    public TopicReader(int threadNum, String group, String topic) {
        this.threadNum = threadNum;
        this.group = group;
        this.topic = topic;
    }

    private void readTopic() {
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        List<KafkaStream<String, String>> streams = ConsumerUtil.getStream(group, topic, threadNum);

        int index = 0;
        for (final KafkaStream<String, String> stream : streams) {
            executor.submit(new ReaderThread(stream, index));
            index++;
        }
    }
}
