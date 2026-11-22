package com.springboot.kafka.controller;

import com.springboot.kafka.dto.Customer;
import com.springboot.kafka.dto.User;
import com.springboot.kafka.service.KafkaMessagePublisher;
import com.springboot.kafka.utils.CsvReaderUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kafka-producer-app")
public class EventPublisher {

    @Autowired
    private KafkaMessagePublisher publisher;

    @GetMapping("/publish/{message}")
    public ResponseEntity<?> sendMessageToTopic(@PathVariable String message) {
        try {
            for (int i = 0; i < 10000; i++) {
                publisher.sendMessageToTopic(message + " " + i);
            }
            return ResponseEntity.ok("message sent successfully...");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }


    @PostMapping("/publish-object")
    public void sendMessageToTopic(@RequestBody Customer customer) {
        try {
            publisher.sendEventToTopic(customer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/publish-partition/{message}")
    public ResponseEntity<?> sendMessageToParticularPartition(@PathVariable String message) {
        try {
            for (int i = 0; i < 100; i++) {
                publisher.sendMessageToParticularPartition(message + " " + i);
            }
            return ResponseEntity.ok("message sent successfully...");
        } catch (Exception ex) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**Here we are sending the csv file to the consumer while some of the user ip is
     * restricted in the consumer, so it will go to the dlt(dead letter topic) and
     * also to the retry topic based on the number of retries defined*/
    @PostMapping("/publish-error-handle")
    public ResponseEntity<?> publishEvent(@RequestBody User user) {
        try {
            List<User> users = CsvReaderUtils.readDataFromCsv();
            users.forEach(usr -> publisher.sendEventsForErrorHandle(usr));
            return ResponseEntity.ok("Message published successfully");
        } catch (Exception exception) {
            return ResponseEntity.
                    status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .build();
        }
    }

}
