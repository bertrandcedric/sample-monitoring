package com.monitoring.kafkamonitor.service;

import com.monitoring.kafkamonitor.model.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;

@Service
public class KafkaMetricsService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMetricsService.class);

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    private AdminClient adminClient;

    @PostConstruct
    private void initializeAdminClient() {
        try {
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
            this.adminClient = AdminClient.create(props);
            logger.info("Kafka AdminClient initialized with bootstrap servers: {}", bootstrapServers);
        } catch (Exception e) {
            logger.error("Failed to initialize Kafka AdminClient: {}", e.getMessage());
        }
    }

    public TopicsResponse getSimpleMetrics() {
        try {
            // Get basic cluster info
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            int nodeCount = clusterResult.nodes().get().size();
            
            // Get topic list
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();
            
            // Get detailed topic information for partition and replication analysis
            TopicDetails topicDetails = getTopicDetails(topicNames);
            
            TopicsResponse response = new TopicsResponse()
                .topics(new ArrayList<>(topicNames))
                .topicDetails(topicDetails);
            
            logger.debug("Retrieved simple Kafka metrics: {} brokers, {} topics", nodeCount, topicNames.size());
            
            return response;
            
        } catch (Exception e) {
            logger.error("Error retrieving Kafka metrics: {}", e.getMessage());
            return new TopicsResponse()
                .topics(new ArrayList<>())
                .topicDetails(new TopicDetails().totalPartitions(0).averagePartitionsPerTopic(0.0).maxPartitions(0).minPartitions(0));
        }
    }

    public KafkaMetricsResponse getAdvancedMetrics() {
        try {
            // Get cluster info
            DescribeClusterResult clusterResult = adminClient.describeCluster();
            String clusterId = clusterResult.clusterId().get();
            int brokerCount = clusterResult.nodes().get().size();
            
            // Get topic details with partition information
            ListTopicsResult topicsResult = adminClient.listTopics();
            Set<String> topicNames = topicsResult.names().get();
            
            DescribeTopicsResult topicsDescription = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> topicDescriptions = new HashMap<>();
            
            // Get each topic description individually
            for (String topicName : topicNames) {
                try {
                    TopicDescription description = topicsDescription.topicNameValues().get(topicName).get();
                    topicDescriptions.put(topicName, description);
                } catch (Exception e) {
                    logger.warn("Could not get description for topic {}: {}", topicName, e.getMessage());
                }
            }
            
            int underReplicatedPartitions = 0;
            int offlinePartitions = 0;
            
            for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
                TopicDescription description = entry.getValue();
                
                for (TopicPartitionInfo partition : description.partitions()) {
                    // Check if partition is under-replicated
                    if (partition.isr().size() < partition.replicas().size()) {
                        underReplicatedPartitions++;
                    }
                    
                    // Check if partition is offline (no leader)
                    if (partition.leader() == null) {
                        offlinePartitions++;
                    }
                }
            }
            
            TopicDetails topicDetails = getTopicDetails(topicNames);
            KafkaMetricsResponse.ClusterHealthEnum clusterHealth = 
                (offlinePartitions == 0 && underReplicatedPartitions == 0) ? 
                KafkaMetricsResponse.ClusterHealthEnum.HEALTHY : 
                KafkaMetricsResponse.ClusterHealthEnum.DEGRADED;
            
            return new KafkaMetricsResponse()
                .clusterId(clusterId)
                .brokerCount(brokerCount)
                .topicCount(topicNames.size())
                .topicDetails(topicDetails)
                .underReplicatedPartitions(underReplicatedPartitions)
                .offlinePartitions(offlinePartitions)
                .clusterHealth(clusterHealth);
            
        } catch (Exception e) {
            logger.error("Error retrieving advanced Kafka metrics: {}", e.getMessage());
            return new KafkaMetricsResponse()
                .clusterId("unknown")
                .brokerCount(0)
                .topicCount(0)
                .topicDetails(new TopicDetails().totalPartitions(0).averagePartitionsPerTopic(0.0).maxPartitions(0).minPartitions(0))
                .underReplicatedPartitions(0)
                .offlinePartitions(0)
                .clusterHealth(KafkaMetricsResponse.ClusterHealthEnum.CRITICAL);
        }
    }
    
    private TopicDetails getTopicDetails(Set<String> topicNames) {
        try {
            DescribeTopicsResult topicsDescription = adminClient.describeTopics(topicNames);
            
            int totalPartitions = 0;
            int maxPartitions = 0;
            int minPartitions = Integer.MAX_VALUE;
            
            for (String topicName : topicNames) {
                try {
                    TopicDescription desc = topicsDescription.topicNameValues().get(topicName).get();
                    int partitionCount = desc.partitions().size();
                    totalPartitions += partitionCount;
                    maxPartitions = Math.max(maxPartitions, partitionCount);
                    minPartitions = Math.min(minPartitions, partitionCount);
                } catch (Exception e) {
                    logger.warn("Could not get partition count for topic {}: {}", topicName, e.getMessage());
                }
            }
            
            if (topicNames.isEmpty()) {
                minPartitions = 0;
            }
            
            return new TopicDetails()
                .totalPartitions(totalPartitions)
                .averagePartitionsPerTopic(topicNames.isEmpty() ? 0.0 : (double) totalPartitions / topicNames.size())
                .maxPartitions(maxPartitions)
                .minPartitions(minPartitions);
            
        } catch (Exception e) {
            logger.warn("Error getting topic details: {}", e.getMessage());
            return new TopicDetails()
                .totalPartitions(0)
                .averagePartitionsPerTopic(0.0)
                .maxPartitions(0)
                .minPartitions(0);
        }
    }

    public ConsumerGroupsResponse getConsumerGroupMetrics() {
        try {
            // Get all consumer groups
            ListConsumerGroupsResult consumerGroupsResult = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> consumerGroups = consumerGroupsResult.all().get();
            
            List<ConsumerGroup> groupMetrics = new ArrayList<>();
            
            for (ConsumerGroupListing group : consumerGroups) {
                String groupId = group.groupId();
                
                try {
                    // Get consumer group description
                    DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(Collections.singleton(groupId));
                    ConsumerGroupDescription description = describeResult.describedGroups().get(groupId).get();
                    
                    String coordinator = description.coordinator().host() + ":" + description.coordinator().port();
                    
                    // Get consumer group offsets and lag
                    ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(groupId);
                    Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();
                    
                    // Get latest offsets for lag calculation
                    Set<TopicPartition> topicPartitions = offsets.keySet();
                    Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
                    for (TopicPartition tp : topicPartitions) {
                        offsetSpecs.put(tp, OffsetSpec.latest());
                    }
                    
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = 
                        adminClient.listOffsets(offsetSpecs).all().get();
                    
                    List<ConsumerPartition> partitionLags = new ArrayList<>();
                    long totalLag = 0;
                    
                    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        OffsetAndMetadata offsetMetadata = entry.getValue();
                        
                        if (offsetMetadata != null && latestOffsets.containsKey(tp)) {
                            long currentOffset = offsetMetadata.offset();
                            long latestOffset = latestOffsets.get(tp).offset();
                            long lag = latestOffset - currentOffset;
                            totalLag += lag;
                            
                            ConsumerPartition partitionInfo = new ConsumerPartition()
                                .topic(tp.topic())
                                .partition(tp.partition())
                                .lag(lag);
                            
                            partitionLags.add(partitionInfo);
                        }
                    }
                    
                    ConsumerGroup consumerGroup = new ConsumerGroup()
                        .groupId(groupId)
                        .state(ConsumerGroup.StateEnum.fromValue(description.state().toString()))
                        .memberCount(description.members().size())
                        .coordinator(coordinator)
                        .totalLag(totalLag)
                        .partitions(partitionLags);
                    
                    groupMetrics.add(consumerGroup);
                    
                } catch (Exception e) {
                    logger.warn("Error getting details for consumer group {}: {}", groupId, e.getMessage());
                    ConsumerGroup errorGroup = new ConsumerGroup()
                        .groupId(groupId)
                        .state(ConsumerGroup.StateEnum.DEAD)
                        .memberCount(0)
                        .coordinator("unknown")
                        .totalLag(0L)
                        .partitions(new ArrayList<>());
                    
                    groupMetrics.add(errorGroup);
                }
            }
            
            return new ConsumerGroupsResponse()
                .totalGroups(consumerGroups.size())
                .consumerGroups(groupMetrics);
            
        } catch (Exception e) {
            logger.error("Error retrieving consumer group metrics: {}", e.getMessage());
            return new ConsumerGroupsResponse()
                .totalGroups(0)
                .consumerGroups(new ArrayList<>());
        }
    }
}
