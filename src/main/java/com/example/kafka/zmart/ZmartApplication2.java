package com.example.kafka.zmart;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import com.example.kafka.util.LogUtils;

public class ZmartApplication2 {

    private static final int branchCount = 5;

    private static long count = 5;

    public static void main(String[] args) throws Exception {
        LogUtils.changeLogLevel();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.3.198:9092");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kstream = streamsBuilder
            // 从事务topic读取消息，使用自定义序列化/反序列化
            .stream("src-topic", Consumed.with(stringSerde, stringSerde));

        // KStream<String, String> kstream2 = kstream.map((key, value) -> {
        // KeyValue<String, String> kv = new KeyValue<>(key, value.toLowerCase());
        // return kv;
        // });

        kstream = kstream.map((key, value) -> new KeyValue<>(String.valueOf(count++ % branchCount), value));

        Predicate[] ps = new Predicate[branchCount];
        for (int i = 0; i < branchCount; i++) {
            final int cnt = i;
            Predicate<String, String> p = new Predicate<String, String>() {
                @Override
                public boolean test(String key, String value) {
                    return Integer.parseInt(key) % branchCount == cnt;
                }
            };
            ps[i] = p;
        }

        KStream<String, String>[] branch = kstream.branch(ps);

        branch[0].foreach((k, v) -> {
            System.out.println(System.currentTimeMillis() + " branch0 -> key=" + k + ", value=" + v);
        });

        branch[1].foreach((k, v) -> {
            System.out.println(System.currentTimeMillis() + " branch1 -> key=" + k + ", value=" + v);
        });

        branch[2].foreach((k, v) -> {
            System.out.println(System.currentTimeMillis() + " branch2 -> key=" + k + ", value=" + v);
        });

        branch[3].foreach((k, v) -> {
            System.out.println(System.currentTimeMillis() + " branch3 -> key=" + k + ", value=" + v);
        });

        branch[4].foreach((k, v) -> {
            System.out.println(System.currentTimeMillis() + " branch4 -> key=" + k + ", value=" + v);
        });

        // KStream<String, String> kstream3 = kstream.map((key, value) -> {
        // KeyValue<String, String> kv = new KeyValue<>(key, value.toUpperCase());
        // return kv;
        // });

        // kstream2.foreach((key, value) -> {
        // System.out.println(System.currentTimeMillis() + ":kstream2----" + key + ", " + value);
        // });

        // kstream3.foreach((key, value) -> {
        // System.out.println(System.currentTimeMillis() + ":kstream3----" + key + ", " + value);
        // });

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();

        try {
            TimeUnit.MINUTES.sleep(15);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        kafkaStreams.close();
    }

}
