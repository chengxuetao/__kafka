package com.example.kafka;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.3.198:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Random rd = new Random();
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            long i = 0;
            for (;;) {
                TimeUnit.SECONDS.sleep(rd.nextInt(5));
                ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("src-topic", null, "The test message " + i++);
                producer.send(producerRecord);
            }
        } catch (Exception e) {

        } finally {
            producer.close();
        }
    }
}
