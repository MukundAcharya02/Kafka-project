package com.mukund.EmailNotificationMicroservice.handler;

import com.mukund.core.ProductEventCreate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @KafkaHandler
    public void handle(ProductEventCreate productEventCreate) {
        System.out.println("********* "+productEventCreate.getTitle());
        log.info("Received a new event: {} {} {}" , productEventCreate.getTitle(),productEventCreate.getProductId(),productEventCreate.getPrice());
    }
}
