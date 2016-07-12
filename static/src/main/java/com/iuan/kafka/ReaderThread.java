package com.iuan.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Created by iuan on 2016/7/9.
 */
public class ReaderThread implements Runnable{
    private KafkaConsumer<String, String> consumer;
    private int batch = 1;

    public ReaderThread(KafkaConsumer<String, String> a_stream, int batch) {
        consumer = a_stream;
        this.batch = batch;
    }

    public void run() {
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(batch);
                for (ConsumerRecord<String, String> record : records){
                    System.out.printf("partition = %d offset = %d, key = %s, value = %s", record.partition(), record.offset(), record.key(), record.value());
                    System.out.println();
                }
            }
        }catch(Exception e) {
            e.printStackTrace();
            System.out.println("Shutting down readerThread");
        }


    }
}
