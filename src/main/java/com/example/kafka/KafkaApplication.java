package com.example.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan({ "com.example.kafka" })
public class KafkaApplication {

    public static void main(final String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
    }

}
