package com.demo.kafka.springbootwithkafka.engine;

import com.demo.kafka.springbootwithkafka.model.User;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class Consumer {

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    @KafkaListener(topics = "test", groupId = "group_id")
    public void consume(String message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        //JSON string to Java object
        User user = mapper.readValue(message, User.class);

        logger.info(String.format("#### -> Consumed message -> %s", message));
        logger.info(String.format("#### -> Name -> %s Age -> %d", user.getName(), user.getAge()));
    }
}
