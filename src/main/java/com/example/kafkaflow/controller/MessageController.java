package com.example.kafkaflow.controller;

import com.example.kafkaflow.config.AppConfig;
import com.example.kafkaflow.config.KafkaClientManager;
import com.example.kafkaflow.service.AvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/clusters/{clusterId}/topics/{topic}")
@CrossOrigin(origins = "*")
public class MessageController {
    private static final Logger log = LoggerFactory.getLogger(MessageController.class);

    private final KafkaClientManager clientManager;
    private final AppConfig appConfig;
    private final AvroDeserializer avroDeserializer;

    public MessageController(KafkaClientManager clientManager, AppConfig appConfig, AvroDeserializer avroDeserializer) {
        this.clientManager = clientManager;
        this.appConfig = appConfig;
        this.avroDeserializer = avroDeserializer;
    }

    // DTOs
    public record MessageDto(
        int partition,
        long offset,
        long timestamp,
        String timestampType,
        String key,
        String value,
        Map<String, String> headers,
        int keySize,
        int valueSize
    ) {}

    public record ProduceRequest(
        String key,
        String value,
        Integer partition,
        Map<String, String> headers
    ) {}

    public record ProduceResponse(
        boolean success,
        int partition,
        long offset,
        long timestamp,
        String error
    ) {}

    @GetMapping("/messages")
    public List<MessageDto> consumeMessages(
            @PathVariable String clusterId,
            @PathVariable String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(defaultValue = "NEWEST") String seekType,
            @RequestParam(required = false) Long offset,
            @RequestParam(required = false) Long timestamp,
            @RequestParam(defaultValue = "100") Integer limit,
            @RequestParam(defaultValue = "AUTO") String keyDeserializer,
            @RequestParam(defaultValue = "AUTO") String valueDeserializer,
            @RequestParam(required = false) String customSchema,
            @RequestParam(required = false) String searchKeyword) throws Exception {

        int maxLimit = Math.min(500, limit); // cap at 500
        List<MessageDto> messages = new ArrayList<>();
        
        AppConfig.ClusterSettings cluster = appConfig.getCluster(clusterId)
                .orElseThrow(() -> new IllegalArgumentException("Cluster not found: " + clusterId));
        String registryUrl = cluster.schemaRegistry();

        boolean hasKeyword = searchKeyword != null && !searchKeyword.trim().isEmpty();
        String keywordLower = hasKeyword ? searchKeyword.trim().toLowerCase() : null;
        int scannedCount = 0;
        int maxScanCount = 30000; // safety ceiling
        long maxScanTimeMs = 6000; // safety timeout of 6 seconds

        try (KafkaConsumer<byte[], byte[]> consumer = clientManager.createConsumer(clusterId, null)) {
            // 1. Resolve partitions to query
            List<TopicPartition> partitionsToAssign = new ArrayList<>();
            if (partition != null) {
                partitionsToAssign.add(new TopicPartition(topic, partition));
            } else {
                // Get all partitions
                var partitionInfos = consumer.partitionsFor(topic);
                if (partitionInfos == null || partitionInfos.isEmpty()) {
                    return Collections.emptyList();
                }
                for (var info : partitionInfos) {
                    partitionsToAssign.add(new TopicPartition(topic, info.partition()));
                }
            }

            consumer.assign(partitionsToAssign);

            // 2. Fetch current boundaries
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitionsToAssign);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitionsToAssign);

            // 3. Perform seek logic
            switch (seekType.toUpperCase()) {
                case "OLDEST":
                    for (TopicPartition tp : partitionsToAssign) {
                        consumer.seek(tp, beginningOffsets.get(tp));
                    }
                    break;

                case "NEWEST":
                    // Consume the last N messages from each partition
                    for (TopicPartition tp : partitionsToAssign) {
                        long begin = beginningOffsets.get(tp);
                        long end = endOffsets.get(tp);
                        // Distribute limit across active partitions, or fetch last N on each
                        long target = Math.max(begin, end - maxLimit);
                        consumer.seek(tp, target);
                    }
                    break;

                case "OFFSET":
                    if (offset == null) {
                        throw new IllegalArgumentException("Offset parameter is required for OFFSET seekType");
                    }
                    for (TopicPartition tp : partitionsToAssign) {
                        long begin = beginningOffsets.get(tp);
                        long end = endOffsets.get(tp);
                        // clamp offset within bounds to avoid OutOfRange Exception
                        long target = Math.max(begin, Math.min(end, offset));
                        consumer.seek(tp, target);
                    }
                    break;

                case "TIMESTAMP":
                    if (timestamp == null) {
                        throw new IllegalArgumentException("Timestamp parameter is required for TIMESTAMP seekType");
                    }
                    Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
                    for (TopicPartition tp : partitionsToAssign) {
                        timestampsToSearch.put(tp, timestamp);
                    }
                    var offsetsForTimes = consumer.offsetsForTimes(timestampsToSearch);
                    for (TopicPartition tp : partitionsToAssign) {
                        var offsetAndTimestamp = offsetsForTimes.get(tp);
                        if (offsetAndTimestamp != null) {
                            consumer.seek(tp, offsetAndTimestamp.offset());
                        } else {
                            // If timestamp is past latest, seek to end
                            consumer.seek(tp, endOffsets.get(tp));
                        }
                    }
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported seekType: " + seekType);
            }

            // 4. Poll messages
            // Calculate per-partition quota to ensure all partitions are represented fairly.
            // When doing keyword search, we ignore the quota and scan freely.
            int numPartitions = partitionsToAssign.size();
            int perPartitionQuota = hasKeyword
                    ? maxLimit
                    : Math.max(1, (int) Math.ceil((double) maxLimit / numPartitions));

            // Fix NEWEST seek: seek each partition back by its own quota, not the full limit
            if ("NEWEST".equalsIgnoreCase(seekType) && !hasKeyword) {
                for (TopicPartition tp : partitionsToAssign) {
                    long begin = beginningOffsets.get(tp);
                    long end = endOffsets.get(tp);
                    long target = Math.max(begin, end - perPartitionQuota);
                    consumer.seek(tp, target);
                }
            }

            // Track per-partition collected counts
            Map<Integer, Integer> partitionMsgCount = new HashMap<>();
            for (TopicPartition tp : partitionsToAssign) {
                partitionMsgCount.put(tp.partition(), 0);
            }

            long startMs = System.currentTimeMillis();
            boolean keepPolling = true;
            
            while (keepPolling && messages.size() < maxLimit && (System.currentTimeMillis() - startMs) < (hasKeyword ? maxScanTimeMs : 2500)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(300));
                if (records.isEmpty()) {
                    // Stop polling if we got results, or wait up to 2s if empty
                    if (!messages.isEmpty()) {
                        keepPolling = false;
                    } else if (System.currentTimeMillis() - startMs > 2000) {
                        keepPolling = false;
                    }
                } else {
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        scannedCount++;
                        if (scannedCount >= maxScanCount) {
                            keepPolling = false;
                            break;
                        }

                        // Skip records from partitions that have already hit their quota
                        // (only enforced when not doing a keyword scan)
                        if (!hasKeyword) {
                            int pCount = partitionMsgCount.getOrDefault(record.partition(), 0);
                            if (pCount >= perPartitionQuota) {
                                continue;
                            }
                        }

                        // Decode Key and Value
                        String keyStr = deserializePayload(record.key(), keyDeserializer, registryUrl, customSchema);
                        String valStr = deserializePayload(record.value(), valueDeserializer, registryUrl, customSchema);

                        // Parse Headers
                        Map<String, String> headersMap = new LinkedHashMap<>();
                        if (record.headers() != null) {
                            for (Header header : record.headers()) {
                                headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                            }
                        }

                        // Keyword match filter
                        boolean isMatch = true;
                        if (hasKeyword) {
                            isMatch = false;
                            if (keyStr != null && keyStr.toLowerCase().contains(keywordLower)) {
                                isMatch = true;
                            } else if (valStr != null && valStr.toLowerCase().contains(keywordLower)) {
                                isMatch = true;
                            } else {
                                // check headers
                                for (Map.Entry<String, String> entry : headersMap.entrySet()) {
                                    if (entry.getKey().toLowerCase().contains(keywordLower) || 
                                        (entry.getValue() != null && entry.getValue().toLowerCase().contains(keywordLower))) {
                                        isMatch = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if (isMatch) {
                            messages.add(new MessageDto(
                                record.partition(),
                                record.offset(),
                                record.timestamp(),
                                record.timestampType().toString(),
                                keyStr,
                                valStr,
                                headersMap,
                                record.key() != null ? record.key().length : 0,
                                record.value() != null ? record.value().length : 0
                            ));
                            partitionMsgCount.merge(record.partition(), 1, Integer::sum);

                            if (messages.size() >= maxLimit) {
                                keepPolling = false;
                                break;
                            }
                        }
                    }

                    // Check if all partitions have hit their per-partition quota
                    if (!hasKeyword) {
                        boolean allFull = partitionMsgCount.values().stream()
                                .allMatch(c -> c >= perPartitionQuota);
                        if (allFull) {
                            keepPolling = false;
                        }
                    }
                }
            }
        }

        // Sort messages: newest timestamp/offset first for easy viewing
        messages.sort(Comparator.comparing(MessageDto::partition)
                .thenComparing(MessageDto::offset).reversed());

        return messages;
    }

    @PostMapping("/produce")
    public ResponseEntity<ProduceResponse> produceMessage(
            @PathVariable String clusterId,
            @PathVariable String topic,
            @RequestBody ProduceRequest request) {
        
        try {
            KafkaProducer<byte[], byte[]> producer = clientManager.getProducer(clusterId);
            
            byte[] keyBytes = request.key() != null ? request.key().getBytes(StandardCharsets.UTF_8) : null;
            byte[] valBytes = request.value() != null ? request.value().getBytes(StandardCharsets.UTF_8) : new byte[0];

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                topic,
                request.partition(),
                keyBytes,
                valBytes
            );

            // Add headers
            if (request.headers() != null) {
                request.headers().forEach((k, v) -> {
                    if (v != null) {
                        record.headers().add(new RecordHeader(k, v.getBytes(StandardCharsets.UTF_8)));
                    }
                });
            }

            RecordMetadata meta = producer.send(record).get(6, TimeUnit.SECONDS);
            log.info("Produced message to topic={}, partition={}, offset={}", topic, meta.partition(), meta.offset());

            return ResponseEntity.ok(new ProduceResponse(
                true,
                meta.partition(),
                meta.offset(),
                meta.timestamp(),
                null
            ));
        } catch (Exception e) {
            log.error("Failed to produce message to topic: " + topic, e);
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            return ResponseEntity.ok(new ProduceResponse(
                false,
                -1,
                -1,
                -1,
                cause.getMessage()
            ));
        }
    }

    /**
     * Decode a binary payload using specified deserializer.
     */
    private String deserializePayload(byte[] data, String deserializer, String registryUrl, String customSchema) {
        if (data == null) {
            return null;
        }

        String mode = deserializer.toUpperCase();
        
        // AUTO detection
        if ("AUTO".equals(mode)) {
            // Check if it's Avro Confluent format (Magic byte)
            if (data.length >= 5 && data[0] == 0 && registryUrl != null && !registryUrl.isEmpty()) {
                mode = "AVRO";
            } else {
                // Try JSON string or standard string
                String str = avroDeserializer.tryStringOrHex(data);
                if (str.trim().startsWith("{") || str.trim().startsWith("[")) {
                    // Looks like JSON, verify if valid JSON formatting
                    try {
                        new com.fasterxml.jackson.databind.ObjectMapper().readTree(str);
                        return str; // Return parsed string directly
                    } catch (Exception ignored) {}
                }
                return str;
            }
        }

        switch (mode) {
            case "STRING":
                return new String(data, StandardCharsets.UTF_8);
            
            case "HEX":
                StringBuilder sb = new StringBuilder("0x");
                for (byte b : data) {
                    sb.append(String.format("%02X", b));
                }
                return sb.toString();

            case "AVRO":
                return avroDeserializer.deserialize(data, registryUrl, customSchema);

            default:
                return avroDeserializer.tryStringOrHex(data);
        }
    }
}
