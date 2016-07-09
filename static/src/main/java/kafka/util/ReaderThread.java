package kafka.util;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * Created by iuan on 2016/7/9.
 */
public class ReaderThread implements Runnable{
    private KafkaStream<String, String> m_stream;
    private int m_threadNumber;

    public ReaderThread(KafkaStream<String, String> a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {
        ConsumerIterator<String, String> it = m_stream.iterator();
        while (it.hasNext())
            System.out.println("Thread " + m_threadNumber + ": " + it.next().message());
        System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
