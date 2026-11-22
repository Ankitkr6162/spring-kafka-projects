package com.springboot.kafka.service;

import com.springboot.kafka.dto.Customer;
import com.springboot.kafka.dto.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    @Autowired
    private KafkaTemplate<String, Object> template;

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaMessagePublisher.class);

    // Here we are sending the message to the defined topic using Kafka Template
    // Here the message is in String
    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> send = template.send("spring-kafka-topic-v3", message);
        send.whenComplete((result,ex) -> {
            if(ex == null){
                System.out.println("Sent message: "+message+" with offset "+result.getRecordMetadata().offset());
            }
            else {
                System.out.println("unable to send message :"+message+" due to "+ex.getMessage());
            }
        });

    }



    // Here the message type is object.
    public void sendEventToTopic(Customer customer) {

        CompletableFuture<SendResult<String, Object>> send =
                template.send("kafka-object-topic", customer);

        send.whenComplete((result, ex) -> {
            if (ex == null) {
                logger.info(
                        "Customer sent successfully. Offset: {}",
                        result.getRecordMetadata().offset()
                );
            } else {
                logger.error("Failed to send Customer to Kafka", ex);
            }
        });
    }

    // It is sending message to different partitions
    // in the consumer it is defined to receive message from only partition 3
    // but only consumer is accepting message from partition 3
    public void sendMessageToParticularPartition(String message) {

        template.send("kafka-partition-topic",2,null,message);
        template.send("kafka-partition-topic",1,null,message);
        template.send("kafka-partition-topic",3,null,message);
        template.send("kafka-partition-topic",4,null,message);

    }


    public void sendEventsForErrorHandle(User user) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("kafka-error-handle-topic", user);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Sent message=[" + user.toString() +
                            "] with offset=[" + result.getRecordMetadata().offset() + "]");
                } else {
                    System.out.println("Unable to send message=[" +
                            user.toString() + "] due to : " + ex.getMessage());
                }
            });
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

}
