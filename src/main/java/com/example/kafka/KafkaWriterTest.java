package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

public class KafkaWriterTest {

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "172.16.3.88:");//slb地址
            // props.put("bootstrap.servers", "120.92.50.146:19092,120.131.2.48:19092,120.92.50.232:19092");
            props.put("acks", "all");
            props.put("retries", 0);
            props.put("batch.size", 16384);
            props.put("linger.ms", 1);
            props.put("buffer.memory", 33554432);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, Object> producer = new KafkaProducer<String, Object>(props);
            String topicName = "test";
            AdminClient adminClient = AdminClient.create(props);
            // List<String> delTopics = new ArrayList<>();
            // delTopics.add("test");
            // delTopics.add("Record_");
            // adminClient.deleteTopics(delTopics);

            ListTopicsResult listTopicsResult = adminClient.listTopics();
            KafkaFuture<Set<String>> names = listTopicsResult.names();
            Set<String> topics = (Set) names.get();
            if (!CollectionUtils.isEmpty(topics)) {
                System.out.println("==========================");
                for (String topic : topics) {
                    System.out.println(topic);
                }
                System.out.println("==========================");
            }

            new Thread(() -> {
                Properties props1 = new Properties();

                props1.put("bootstrap.servers", "120.92.16.85:19092");
                props1.put("group.id", "test_group");
                props1.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                props1.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

                try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props1)) {
                    consumer.subscribe(Arrays.asList("test"));
                    while (true) {
                        ConsumerRecords<String, Object> records = consumer.poll(1);
                        for (ConsumerRecord<String, Object> record : records) {
                            Object value = record.value();
                            System.err.println("consumer value===============>" + value);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();

            Scanner sc = new Scanner(System.in);
            System.out.println("Please Enter Message:");
            String message = sc.nextLine();
            while (StringUtils.hasText(message)) {
                producer.send(new ProducerRecord<String, Object>(topicName, null, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                        }
                    }
                });
                System.out.println("Please Enter Message:");
                message = sc.nextLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
