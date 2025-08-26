package com.monitoring.kafkamonitor.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    
    @KafkaListener(topics = {"test-topic", "metrics-topic", "monitoring-events"}, groupId = "kafka-monitor")
    public void consumeMessages(@Payload String message,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.OFFSET) long offset,
                               @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key) {
        
        logger.info("Received message from topic=[{}], partition=[{}], offset=[{}], key=[{}]: {}", 
                   topic, partition, offset, key, message);
        
        // Process the message here
        processMessage(topic, message);
    }
    
    private void processMessage(String topic, String message) {
        // Add your message processing logic here
        logger.debug("Processing message from topic [{}]: {}", topic, message);
    }
}