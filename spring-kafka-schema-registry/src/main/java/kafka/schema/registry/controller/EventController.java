package kafka.schema.registry.controller;

import kafka.schema.registry.dto.Employee;
import kafka.schema.registry.producer_app.KafkaAvroProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka-schema-registry")
public class EventController {

    /** Avro is being used to for version control of object delivered from
     * producer to consumer, this helps maintain the object version and provides
     * the flexibility to change the object schema without breaking the pub-sub flow.*/

    /** docker properties are defined -> docker compose up -d to run the required containers
     * http://localhost:9021/clusters UI view of the confluent to see the topics, messages and schema version
     * after defining the .avsc file run the Gradle build task to create the object of the avsc file fields
     * we have to run the Gradle build everytime if there is change in the .avsc file then rerun the app. */

    @Autowired
    private KafkaAvroProducer producer;

    @PostMapping("/events")
    public String sendMessage(@RequestBody Employee employee) {
        producer.send(employee);
        return "message published !";
    }

}
