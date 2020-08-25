package com.example.kafka.zmart;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.example.kafka.util.LogUtils;

public class ZmartApplication {

    private static int count = 1;

    public static void main(String[] args) throws Exception {
        LogUtils.changeLogLevel();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "zmart");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.3.198:9092");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> kstream = streamsBuilder
            // 从事务topic读取消息，使用自定义序列化/反序列化
            .stream("8a7a80916b214db7016b217cb1fb0006", Consumed.with(stringSerde, stringSerde));

        kstream = kstream.mapValues((value) -> value + ", " + (count++));

        for (int i = 0; i < 2; i++) {
            final String name = "stream" + i;
            KStream<String, String> metricStream = kstream.filter((key, record) -> true);
            metricStream.foreach((k, v) -> {
                System.out.println(name + " - key=" + k + ", value=" + v);
            });
        }

        for (int i = 2; i < 4; i++) {
            final String name = "stream" + i;
            KStream<String, String> metricStream = kstream.filter((key, record) -> true);
            metricStream.foreach((k, v) -> {
                System.out.println(name + " - key=" + k + ", value=" + v);
            });
        }

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
        kafkaStreams.start();
    }

}
