package com.example.kafka;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaLog {

    private static final String topic = "8a7a80906b0d6af0016b0d6c332d0005";

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "172.16.3.198:9094");
        props.put("group.id", topic + "_group");
        // props.put("auto.commit.interval.ms", "1000");
        // props.put("session.timeout.ms", "30000");
        // props.put("max.poll.records", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        long preLagSum = 0l;

        for (;;) {

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Arrays.asList(topic));
                List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
                List<PartitionInfo> partitions = consumer.partitionsFor(topic);
                for (PartitionInfo partition : partitions) {
                    TopicPartition topicPartition = new TopicPartition(partition.topic(), partition.partition());
                    topicPartitions.add(topicPartition);
                }
                Map<Integer, Long> endOffsetMap = new HashMap<Integer, Long>();
                Map<Integer, Long> commitOffsetMap = new HashMap<Integer, Long>();
                // 查询log size
                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
                for (TopicPartition partitionInfo : endOffsets.keySet()) {
                    endOffsetMap.put(partitionInfo.partition(), endOffsets.get(partitionInfo));
                }

                // 查询消费offset
                for (TopicPartition topicAndPartition : topicPartitions) {
                    OffsetAndMetadata committed = consumer.committed(topicAndPartition);
                    commitOffsetMap.put(topicAndPartition.partition(), committed.offset());
                }
                // 累加lag
                long lagSum = 0l;
                if (endOffsetMap.size() == commitOffsetMap.size()) {
                    for (Integer partition : endOffsetMap.keySet()) {
                        long endOffSet = endOffsetMap.get(partition);
                        long commitOffSet = commitOffsetMap.get(partition);
                        long diffOffset = endOffSet - commitOffSet;
                        lagSum += diffOffset;
                        // System.out.println("Topic:coredata, groupID:coredata_group, partition:" + partition + ",
                        // endOffset:" + endOffSet + ", commitOffset:"
                        // + commitOffSet + ", diffOffset:" + diffOffset);
                    }
                    File file = new File("D:\\log.txt");
                    Writer w = new FileWriter(file, true);
                    String result = "Time:" + new Date() + "Topic:coredata, groupID:coredata_group, PROCESS_LAG:"
                        + ((preLagSum - lagSum) < 0 ? 0 : (preLagSum - lagSum)) + ", LAG:" + lagSum + "\r\n";
                    preLagSum = lagSum;
                    w.write(result);
                    w.close();
                } else {
                    System.out.println("this topic partitions lost");
                }
                // consumer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
