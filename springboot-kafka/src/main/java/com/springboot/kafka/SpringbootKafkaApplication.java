package com.springboot.kafka;
import java.util.Map;

import com.springboot.kafka.dto.Customer;
import com.springboot.kafka.dto.User;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;


@SpringBootApplication
public class SpringbootKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKafkaApplication.class, args);
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(
                kafkaProperties.buildProducerProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(Map.of(
                        byte[].class, new ByteArraySerializer(),
                        User.class, new JacksonJsonSerializer<>(),
                        Customer.class, new JacksonJsonSerializer<>()
                ))
        );
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
