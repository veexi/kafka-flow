package com.example.kafkaflow.controller;

import com.example.kafkaflow.config.KafkaClientManager;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/clusters/{clusterId}/groups")
@CrossOrigin(origins = "*")
public class ConsumerGroupController {
    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupController.class);

    private final KafkaClientManager clientManager;

    public ConsumerGroupController(KafkaClientManager clientManager) {
        this.clientManager = clientManager;
    }

    // DTOs
    public record GroupSummaryDto(
        String groupId,
        String state,
        boolean isSimpleConsumerGroup,
        int membersCount
    ) {}

    public record PartitionLagDto(
        String topic,
        int partition,
        long currentOffset,
        long logEndOffset,
        long lag
    ) {}

    public record MemberAssignmentDto(
        String memberId,
        String clientId,
        String host,
        List<TopicPartitionDto> assignments
    ) {}

    public record TopicPartitionDto(
        String topic,
        int partition
    ) {}

    public record GroupDetailDto(
        String groupId,
        String state,
        String protocolType,
        String coordinatorHost,
        List<MemberAssignmentDto> members,
        List<PartitionLagDto> partitionLags,
        long totalLag
    ) {}

    @GetMapping
    public List<GroupSummaryDto> listGroups(@PathVariable String clusterId) throws Exception {
        AdminClient admin = clientManager.getAdminClient(clusterId);
        
        Collection<ConsumerGroupListing> listings = admin.listConsumerGroups().all().get(8, TimeUnit.SECONDS);
        if (listings.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
        Map<String, ConsumerGroupDescription> descriptions = admin.describeConsumerGroups(groupIds).all().get(8, TimeUnit.SECONDS);

        return listings.stream()
                .map(listing -> {
                    String groupId = listing.groupId();
                    ConsumerGroupDescription desc = descriptions.get(groupId);
                    int members = desc != null ? desc.members().size() : 0;
                    String state = desc != null ? desc.state().toString() : "UNKNOWN";

                    return new GroupSummaryDto(
                        groupId,
                        state,
                        listing.isSimpleConsumerGroup(),
                        members
                    );
                })
                .sorted(Comparator.comparing(GroupSummaryDto::groupId))
                .collect(Collectors.toList());
    }

    @GetMapping("/{groupId}")
    public GroupDetailDto getGroupDetail(@PathVariable String clusterId, @PathVariable String groupId) throws Exception {
        AdminClient admin = clientManager.getAdminClient(clusterId);

        // 1. Fetch group description (members, state, coordinator)
        ConsumerGroupDescription desc = admin.describeConsumerGroups(Collections.singletonList(groupId))
                .all().get(8, TimeUnit.SECONDS).get(groupId);

        if (desc == null) {
            throw new IllegalArgumentException("Consumer Group not found: " + groupId);
        }

        // 2. Fetch group committed offsets
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(8, TimeUnit.SECONDS);

        List<TopicPartition> topicPartitions = new ArrayList<>(committedOffsets.keySet());

        // Add assignments from active members that might not have committed offsets yet
        desc.members().forEach(member -> {
            if (member.assignment() != null && member.assignment().topicPartitions() != null) {
                for (TopicPartition tp : member.assignment().topicPartitions()) {
                    if (!topicPartitions.contains(tp)) {
                        topicPartitions.add(tp);
                    }
                }
            }
        });

        // 3. Query Log End Offsets (latest broker offsets) using a temp consumer
        Map<TopicPartition, Long> logEndOffsets = new HashMap<>();
        if (!topicPartitions.isEmpty()) {
            try (KafkaConsumer<byte[], byte[]> consumer = clientManager.createConsumer(clusterId, null)) {
                Map<TopicPartition, Long> ends = consumer.endOffsets(topicPartitions);
                logEndOffsets.putAll(ends);
            }
        }

        // 4. Build Lag Table
        List<PartitionLagDto> partitionLags = new ArrayList<>();
        long totalLag = 0;

        for (TopicPartition tp : topicPartitions) {
            long currentOffset = -1;
            OffsetAndMetadata metadata = committedOffsets.get(tp);
            if (metadata != null) {
                currentOffset = metadata.offset();
            }

            long endOffset = logEndOffsets.getOrDefault(tp, 0L);
            long lag = 0;
            if (currentOffset >= 0) {
                lag = Math.max(0, endOffset - currentOffset);
            } else {
                lag = endOffset; // If never committed, lag is the total message count
            }
            
            totalLag += lag;

            partitionLags.add(new PartitionLagDto(
                tp.topic(),
                tp.partition(),
                currentOffset,
                endOffset,
                lag
            ));
        }

        // Sort lag items by topic and partition
        partitionLags.sort(Comparator.comparing(PartitionLagDto::topic).thenComparing(PartitionLagDto::partition));

        // 5. Build Member DTOs
        List<MemberAssignmentDto> members = desc.members().stream()
                .map(m -> {
                    List<TopicPartitionDto> assigns = m.assignment().topicPartitions().stream()
                            .map(tp -> new TopicPartitionDto(tp.topic(), tp.partition()))
                            .sorted(Comparator.comparing(TopicPartitionDto::topic).thenComparing(TopicPartitionDto::partition))
                            .collect(Collectors.toList());

                    return new MemberAssignmentDto(
                        m.consumerId(),
                        m.clientId(),
                        m.host(),
                        assigns
                    );
                })
                .sorted(Comparator.comparing(MemberAssignmentDto::memberId))
                .collect(Collectors.toList());

        String coordHost = desc.coordinator() != null ? desc.coordinator().host() + ":" + desc.coordinator().port() : "UNKNOWN";

        return new GroupDetailDto(
            desc.groupId(),
            desc.state().toString(),
            desc.partitionAssignor(),
            coordHost,
            members,
            partitionLags,
            totalLag
        );
    }
}
