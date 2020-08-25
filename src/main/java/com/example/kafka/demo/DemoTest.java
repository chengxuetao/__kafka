package com.example.kafka.demo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.example.kafka.util.LogUtils;

public class DemoTest {

    public static void main(String[] args) throws Exception {
        LogUtils.changeLogLevel();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "hello-world");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.3.198:9092");

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> simpleFirstStream =
            builder.stream("src-topic", Consumed.with(stringSerde, stringSerde));
        // 使用KStream.mapValues方法把每行输入转换为大写
        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues(line -> line.toUpperCase());
        upperCasedStream.foreach((key, val) -> {
            System.out.println("key=" + key + ", value=" + val);
        });
        // 把转换结果输出到另一个topic
        // upperCasedStream.to("out-topic", Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
        System.in.read();
    }

}
