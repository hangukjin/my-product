package com.example.demo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;

@Service
public class KafkaListener {
    @Autowired
    ProductRepository productRepository;

    @StreamListener(Processor.INPUT)
    public void onEventByString(@Payload OrderPlaced orderPlaced){
        if( orderPlaced.getEventType().equals("OrderPlaced")){
            System.out.println("======================");
            System.out.println(orderPlaced.getProductName());
            System.out.println(orderPlaced.getQty());
            productRepository.findById(orderPlaced.getProductId())
                    .ifPresent(
                            product -> {
                                product.setStock(orderPlaced.getQty());
                                productRepository.save(product);
                            }
                    )
            ;

            System.out.println("======================");
        }
    }
}
