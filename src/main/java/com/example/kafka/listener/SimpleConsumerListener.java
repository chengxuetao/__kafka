package com.example.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SimpleConsumerListener {

    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumerListener.class);

    // private final CountDownLatch latch1 = new CountDownLatch(1);

    @KafkaListener(id = "foo", topics = "topic-test")
    public void listen(String records) {
        logger.info("=========>" + records);
        // this.latch1.countDown();
    }

}