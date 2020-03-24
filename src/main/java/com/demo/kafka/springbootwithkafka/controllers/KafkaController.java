package com.demo.kafka.springbootwithkafka.controllers;

import com.demo.kafka.springbootwithkafka.engine.Producer;
import com.demo.kafka.springbootwithkafka.model.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaController {

    private final Producer producer;

    @Autowired
    KafkaController(Producer producer) {
        this.producer = producer;
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
        this.producer.sendMessage(message);
    }

    @PostMapping(value = "/create")
    public User sendMessageToKafkaTopic(@RequestBody User message) throws JsonProcessingException {

        ObjectMapper Obj = new ObjectMapper();
        String jsonStr = Obj.writeValueAsString(message);
        this.producer.sendMessage(jsonStr);

        return message;
    }
}
