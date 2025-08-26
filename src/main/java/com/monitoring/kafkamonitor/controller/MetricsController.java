package com.monitoring.kafkamonitor.controller;

import com.monitoring.kafkamonitor.api.KafkaMetricsApi;
import com.monitoring.kafkamonitor.model.ConsumerGroupsResponse;
import com.monitoring.kafkamonitor.model.HealthStatusResponse;
import com.monitoring.kafkamonitor.model.KafkaMetricsResponse;
import com.monitoring.kafkamonitor.model.TopicDetails;
import com.monitoring.kafkamonitor.model.TopicsResponse;
import com.monitoring.kafkamonitor.service.KafkaMetricsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricsController implements KafkaMetricsApi {

    @Autowired
    private KafkaMetricsService kafkaMetricsService;

    @Override
    public ResponseEntity<HealthStatusResponse> getHealthStatus() {
        try {
            KafkaMetricsResponse advancedMetrics = kafkaMetricsService.getAdvancedMetrics();
            
            // Application is UP if we can reach this endpoint
            String applicationStatus = "UP";
            String clusterHealth = advancedMetrics.getClusterHealth().getValue();
            
            HealthStatusResponse response = new HealthStatusResponse()
                .applicationStatus(HealthStatusResponse.ApplicationStatusEnum.fromValue(applicationStatus))
                .clusterHealth(HealthStatusResponse.ClusterHealthEnum.fromValue(clusterHealth));
            
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            HealthStatusResponse errorResponse = new HealthStatusResponse()
                .applicationStatus(HealthStatusResponse.ApplicationStatusEnum.DOWN)
                .clusterHealth(HealthStatusResponse.ClusterHealthEnum.UNKNOWN)
                .error(e.getMessage());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }

    @Override
    public ResponseEntity<TopicsResponse> getTopicInformation() {
        try {
            TopicsResponse response = kafkaMetricsService.getSimpleMetrics();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            TopicsResponse errorResponse = new TopicsResponse()
                .topics(java.util.Collections.emptyList())
                .topicDetails(new TopicDetails().totalPartitions(0).averagePartitionsPerTopic(0.0).maxPartitions(0).minPartitions(0));
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }

    @Override
    public ResponseEntity<ConsumerGroupsResponse> getConsumerGroupMetrics() {
        try {
            ConsumerGroupsResponse response = kafkaMetricsService.getConsumerGroupMetrics();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            ConsumerGroupsResponse errorResponse = new ConsumerGroupsResponse()
                .totalGroups(0)
                .consumerGroups(java.util.Collections.emptyList());
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }

    @Override
    public ResponseEntity<KafkaMetricsResponse> getKafkaMetrics() {
        try {
            KafkaMetricsResponse response = kafkaMetricsService.getAdvancedMetrics();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            KafkaMetricsResponse errorResponse = new KafkaMetricsResponse()
                .clusterId("unknown")
                .brokerCount(0)
                .topicCount(0)
                .topicDetails(new TopicDetails().totalPartitions(0).averagePartitionsPerTopic(0.0).maxPartitions(0).minPartitions(0))
                .underReplicatedPartitions(0)
                .offlinePartitions(0)
                .clusterHealth(KafkaMetricsResponse.ClusterHealthEnum.CRITICAL);
            
            return ResponseEntity.status(500).body(errorResponse);
        }
    }
}
