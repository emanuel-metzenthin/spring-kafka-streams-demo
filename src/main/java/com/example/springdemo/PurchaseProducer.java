package com.example.springdemo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class PurchaseProducer implements ApplicationRunner {
    @Autowired
    RandomPurchaseGenerator randomPurchaseGenerator;

    MessageChannel purchaseOut;

    public PurchaseProducer(Channels channelsBinding) {
        purchaseOut = channelsBinding.purchaseOut();
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(this::sendPurchase, 1, 1, TimeUnit.SECONDS);
    }

    private void sendPurchase() {
        Purchase purchase = randomPurchaseGenerator.generate();

        Message message = MessageBuilder
                .withPayload(purchase)
                .setHeader(KafkaHeaders.MESSAGE_KEY, purchase.getProductName().getBytes())
                .build();

        purchaseOut.send(message);
    }
}