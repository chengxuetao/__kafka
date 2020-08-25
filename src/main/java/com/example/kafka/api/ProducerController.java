package com.example.kafka.api;

import io.swagger.annotations.ApiOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SuppressWarnings({ "rawtypes", "unchecked" })
@RestController
public class ProducerController {

    private final static Logger LOGGER = LoggerFactory.getLogger(ProducerController.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @ApiOperation("product message")
    @RequestMapping(value = "/product/message", method = RequestMethod.GET)
    public String message(@RequestParam String message) throws Exception {
        ListenableFuture<SendResult> send = kafkaTemplate.send("topic-test", 1, message);
        send.addCallback(new ListenableFutureCallback<SendResult>() {
            @Override
            public void onFailure(Throwable e) {
                LOGGER.error("onFailure - ", e);
            }

            @Override
            public void onSuccess(SendResult integerStringSendResult) {
                LOGGER.info("onSuccess - " + integerStringSendResult);
            }
        });
        SendResult sendResult = send.get();
        LOGGER.info("sendResult - " + sendResult);
        return sendResult.toString();
    }
}
