package com.springboot.kafka;

import com.springboot.kafka.dto.Customer;
import com.springboot.kafka.service.KafkaMessagePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class SpringbootKafkaApplicationTests {

    @Container
    static KafkaContainer container = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"));

    @DynamicPropertySource
    public static void initKafkaProperties(DynamicPropertyRegistry registry){
        registry.add("spring.kafka.bootstrap-servers",container::getBootstrapServers);

    }


    @Autowired
    private KafkaMessagePublisher messagePublisher;

    @Test
    public void SendEventsToTopic(){
        messagePublisher.sendEventToTopic(new Customer(234,"test user", "ranchi","8745475."));
        await().pollInterval(Duration.ofSeconds(3)).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {

        });

    }
}
