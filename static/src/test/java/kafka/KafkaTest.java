package kafka;

import com.sun.org.apache.xpath.internal.SourceTree;
import junit.framework.TestCase;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.util.ConsumerUtil;
import kafka.util.ProducerUtil;

import java.util.List;

/**
 * Created by iuan on 2016/7/9.
 */
public class KafkaTest extends TestCase{

    public void testKafka(){
        while(true){
            ProducerUtil.write("test11", null, "123");
            System.out.println("write");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void testRead(){
        TopicReader reader = new TopicReader("group" + System.currentTimeMillis(), "test", 1);
        reader.readTopic();
    }

    public void testRead2(){

    }

}
