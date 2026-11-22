package com.springboot.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig {

    // creating new topic with the help of Apache Kafka NewTopic class
    @Bean
    public NewTopic createNewTopic(){
        return new NewTopic("kafka-error-handle-topic",5,(short) 1);
    }
}
