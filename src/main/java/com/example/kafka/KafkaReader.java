package com.example.kafka;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.kafka.util.LogUtils;

public class KafkaReader {

    private final Logger logger = LoggerFactory.getLogger(KafkaReader.class);

    public static final String CONSUMER = "consumer";

    public static final String TOPIC = "topic";

    private static final SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    private int timeout = 100;

    private String[] topics = null;

    private Properties props = new Properties();

    public KafkaReader(String kafkaAddr, String groupId, String... topics) {
        this.topics = topics;
        props.put("bootstrap.servers", kafkaAddr);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        if (logger.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            for (String topic : topics) {
                builder.append(topic).append(",");
            }
            logger.debug("Start consumer:" + kafkaAddr + "- topic:" + builder.toString() + "- group id:" + groupId);
        }
    }

    public void start() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topics));
            for (;;) {
                ConsumerRecords<String, String> records = consumer.poll(timeout);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        String value = record.value();
                        try {
                            // JSONObject metricObj = JSONObject.parseObject(value);
                            // String metricId = metricObj.getString("metric");
                            System.out.println(sdf.format(new Date()) + " - " + value);
                        } catch (Exception e) {
                            System.err.println("处理数据异常：" + value);
                            logger.error("处理数据异常：", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        LogUtils.changeLogLevel();
        KafkaReader reader = new KafkaReader("172.16.3.198:9092", UUID.randomUUID().toString(), "metrics");
        reader.start();
    }

}
