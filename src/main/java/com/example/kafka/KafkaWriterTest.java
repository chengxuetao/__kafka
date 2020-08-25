package com.example.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

public class KafkaWriterTest {

    public static void main(String[] args) {
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", "221.218.244.50:9093");
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
            List<String> delTopics = new ArrayList<>();
            delTopics.add("test");
            delTopics.add("Record_");
            adminClient.deleteTopics(delTopics);

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
