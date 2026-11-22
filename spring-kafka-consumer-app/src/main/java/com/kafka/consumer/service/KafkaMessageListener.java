package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.consumer.dto.Customer;
import com.kafka.consumer.dto.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class KafkaMessageListener {

    private static final Logger logger =
            LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(
            topics = "spring-kafka-topic-v3",
            groupId = "spring-kafka-consumer-group2"
    )
    public void consumeMessageFromTopic(String message) {

        logger.info("Consumer1 received message: {}", message);
    }

    @KafkaListener(
            topics = "spring-kafka-topic-v3",
            groupId = "spring-kafka-consumer-group2"
    )
    public void consumeMessageFromTopic2(String message) {

        logger.info("Consumer2 received message: {}", message);
    }

    @KafkaListener(
            topics = "spring-kafka-topic-v3",
            groupId = "spring-kafka-consumer-group2"
    )
    public void consumeMessageFromTopic3(String message) {

        logger.info("Consumer3 received message: {}", message);
    }

    @KafkaListener(
            topics = "spring-kafka-topic-v3",
            groupId = "spring-kafka-consumer-group2"
    )
    public void consumeMessageFromTopic4(String message) {

        logger.info("Consumer4 received message: {}", message);
    }

    @KafkaListener(
            topics = "kafka-object-topic",
            groupId = "kafka-object-topic-group"
    )
    public void consumeEventFromTopic(Customer customer) {
        logger.info("Consumer received the event object: {}", customer);
    }

    // here consumer receives the message from only mentioned partition.
    @KafkaListener(
            groupId = "kafka-partition-group",
            topicPartitions = {@TopicPartition(topic = "kafka-partition-topic", partitions = {"3"})}
    )
    public void consumeEventFromParticularPartition(String  message) {
        logger.info("Consumer received the event from particular partition: {}", message);
    }


    @RetryableTopic(attempts = "4")// 3 topic N-1
    @KafkaListener(topics = "kafka-error-handle-topic", groupId = "kafka-error-handle-group")
    public void consumeEventsForErrorHandle(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) throws JsonProcessingException {
        logger.info("Received: {} from {} offset {}", new ObjectMapper().writeValueAsString(user), topic, offset);
        //validate restricted IP before process the records
        List<String> restrictedIpList = Stream.of("32.241.244.236", "15.55.49.164", "81.1.95.253", "126.130.43.183").collect(Collectors.toList());
        //it does not allow to send message from restricted ip's
        if (restrictedIpList.contains(user.getIpAddress())) {
            throw new RuntimeException("Invalid IP Address received !");
        }

    }

    @DltHandler
    public void listenDLT(User user, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.OFFSET) long offset) {
        logger.info("DLT Received : {} , from {} , offset {}",user.getFirstName(),topic,offset);
    }
}
