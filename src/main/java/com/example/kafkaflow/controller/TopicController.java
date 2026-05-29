package com.example.kafkaflow.controller;

import com.example.kafkaflow.config.KafkaClientManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/clusters/{clusterId}/topics")
@CrossOrigin(origins = "*")
public class TopicController {
    private static final Logger log = LoggerFactory.getLogger(TopicController.class);

    private final KafkaClientManager clientManager;

    public TopicController(KafkaClientManager clientManager) {
        this.clientManager = clientManager;
    }

    // DTOs
    public record TopicSummaryDto(
        String name,
        int partitionCount,
        int replicationFactor,
        boolean isInternal
    ) {}

    public record PartitionDetailDto(
        int partition,
        int leaderId,
        List<Integer> replicas,
        List<Integer> isr,
        long startOffset,
        long endOffset,
        long messageCount
    ) {}

    public record TopicDetailDto(
        String name,
        boolean isInternal,
        List<PartitionDetailDto> partitions,
        long totalMessages
    ) {}

    @GetMapping
    public List<TopicSummaryDto> listTopics(@PathVariable String clusterId) throws Exception {
        AdminClient admin = clientManager.getAdminClient(clusterId);
        
        // List all topic names
        Set<String> topicNames = admin.listTopics().names().get(8, TimeUnit.SECONDS);
        if (topicNames.isEmpty()) {
            return Collections.emptyList();
        }

        // Describe topics to get partitions, replicas, and internal flag
        Map<String, TopicDescription> descriptions = admin.describeTopics(topicNames).all().get(8, TimeUnit.SECONDS);
        
        return descriptions.values().stream()
                .map(desc -> {
                    int partCount = desc.partitions().size();
                    int repFactor = partCount > 0 ? desc.partitions().get(0).replicas().size() : 0;
                    return new TopicSummaryDto(
                        desc.name(),
                        partCount,
                        repFactor,
                        desc.isInternal()
                    );
                })
                .sorted(Comparator.comparing(TopicSummaryDto::isInternal) // put internal topics at the bottom
                        .thenComparing(TopicSummaryDto::name))
                .collect(Collectors.toList());
    }

    @GetMapping("/{topic}")
    public TopicDetailDto getTopicDetail(@PathVariable String clusterId, @PathVariable String topic) throws Exception {
        AdminClient admin = clientManager.getAdminClient(clusterId);
        
        // Describe topic to get partition configurations
        TopicDescription desc = admin.describeTopics(Collections.singletonList(topic))
                .all().get(8, TimeUnit.SECONDS).get(topic);

        if (desc == null) {
            throw new IllegalArgumentException("Topic not found: " + topic);
        }

        List<TopicPartition> topicPartitions = desc.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());

        Map<Integer, Long> beginningOffsets = new HashMap<>();
        Map<Integer, Long> endOffsets = new HashMap<>();

        // Fetch offsets using a temporary short-lived consumer
        try (KafkaConsumer<byte[], byte[]> consumer = clientManager.createConsumer(clusterId, null)) {
            // Fetch beginning offsets
            Map<TopicPartition, Long> start = consumer.beginningOffsets(topicPartitions);
            start.forEach((tp, offset) -> beginningOffsets.put(tp.partition(), offset));

            // Fetch end offsets
            Map<TopicPartition, Long> end = consumer.endOffsets(topicPartitions);
            end.forEach((tp, offset) -> endOffsets.put(tp.partition(), offset));
        }

        List<PartitionDetailDto> partitionDetails = desc.partitions().stream()
                .map(p -> {
                    long start = beginningOffsets.getOrDefault(p.partition(), 0L);
                    long end = endOffsets.getOrDefault(p.partition(), 0L);
                    long msgCount = Math.max(0, end - start);
                    
                    List<Integer> replicas = p.replicas().stream().map(node -> node.id()).collect(Collectors.toList());
                    List<Integer> isr = p.isr().stream().map(node -> node.id()).collect(Collectors.toList());
                    int leaderId = p.leader() != null ? p.leader().id() : -1;

                    return new PartitionDetailDto(
                        p.partition(),
                        leaderId,
                        replicas,
                        isr,
                        start,
                        end,
                        msgCount
                    );
                })
                .sorted(Comparator.comparing(PartitionDetailDto::partition))
                .collect(Collectors.toList());

        long totalMessages = partitionDetails.stream().mapToLong(p -> p.messageCount).sum();

        return new TopicDetailDto(
            desc.name(),
            desc.isInternal(),
            partitionDetails,
            totalMessages
        );
    }

    // Request DTO for Creation
    public record CreateTopicRequest(
        String name,
        int partitions,
        short replicationFactor
    ) {}

    @PostMapping
    public ResponseEntity<Map<String, Object>> createTopic(
            @PathVariable String clusterId,
            @RequestBody CreateTopicRequest request) throws Exception {
        log.info("Creating topic '{}' on cluster '{}' (partitions={}, replication={})", 
            request.name(), clusterId, request.partitions(), request.replicationFactor());
            
        AdminClient admin = clientManager.getAdminClient(clusterId);
        org.apache.kafka.clients.admin.NewTopic newTopic = new org.apache.kafka.clients.admin.NewTopic(
            request.name(),
            request.partitions(),
            request.replicationFactor()
        );
        
        admin.createTopics(Collections.singleton(newTopic)).all().get(10, TimeUnit.SECONDS);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "Topic '" + request.name() + "' created successfully.");
        return ResponseEntity.ok(response);
     }

    @DeleteMapping("/{topic}")
    public ResponseEntity<Map<String, Object>> deleteTopic(
            @PathVariable String clusterId,
            @PathVariable String topic) throws Exception {
        log.info("Deleting topic '{}' on cluster '{}'", topic, clusterId);
        
        AdminClient admin = clientManager.getAdminClient(clusterId);
        admin.deleteTopics(Collections.singleton(topic)).all().get(10, TimeUnit.SECONDS);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "Topic '" + topic + "' deleted successfully.");
        return ResponseEntity.ok(response);
    }

    @PostMapping("/{topic}/purge")
    public ResponseEntity<Map<String, Object>> purgeTopic(
            @PathVariable String clusterId,
            @PathVariable String topic) throws Exception {
        log.info("Purging message data in topic '{}' on cluster '{}'", topic, clusterId);
        
        AdminClient admin = clientManager.getAdminClient(clusterId);
        
        // 1. Describe topic to get partition configurations
        TopicDescription desc = admin.describeTopics(Collections.singletonList(topic))
                .all().get(8, TimeUnit.SECONDS).get(topic);
        
        if (desc == null) {
            throw new IllegalArgumentException("Topic not found: " + topic);
        }

        List<TopicPartition> topicPartitions = desc.partitions().stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .collect(Collectors.toList());

        // 2. Fetch current log end offsets (latest offsets)
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = clientManager.createConsumer(clusterId, null)) {
            endOffsets.putAll(consumer.endOffsets(topicPartitions));
        }

        // 3. Construct deleteRecords map (delete up to current end offsets)
        Map<TopicPartition, org.apache.kafka.clients.admin.RecordsToDelete> recordsToDelete = new HashMap<>();
        endOffsets.forEach((tp, offset) -> {
            if (offset > 0) {
                recordsToDelete.put(tp, org.apache.kafka.clients.admin.RecordsToDelete.beforeOffset(offset));
            }
        });

        // 4. Perform delete records operation if there are records to delete
        if (!recordsToDelete.isEmpty()) {
            admin.deleteRecords(recordsToDelete).all().get(10, TimeUnit.SECONDS);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("message", "Topic '" + topic + "' messages purged successfully.");
        return ResponseEntity.ok(response);
    }
}
