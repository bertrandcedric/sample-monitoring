package com.monitoring.kafkamonitor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class KafkaProducerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    @Scheduled(fixedRate = 1000) // Send message every 1 second (1000 milliseconds)
    public void sendMessage() {
        String topic = "test-topic";
        String message = "Scheduled message at " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        
        try {
            kafkaTemplate.send(topic, message);
            logger.info("Sent message to topic=[{}]: {}", topic, message);
        } catch (Exception e) {
            logger.error("Error sending message to topic {}: {}", topic, e.getMessage());
        }
    }
}