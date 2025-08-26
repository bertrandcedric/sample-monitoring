package com.monitoring.kafkamonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaMonitorApplication.class, args);
    }

}