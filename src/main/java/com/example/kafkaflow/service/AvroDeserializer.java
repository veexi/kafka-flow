package com.example.kafkaflow.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AvroDeserializer {
    private static final Logger log = LoggerFactory.getLogger(AvroDeserializer.class);
    
    private static final GenericData GENERIC_DATA = new GenericData();
    static {
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.Conversions.UUIDConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimeMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimeMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion());
        GENERIC_DATA.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion());
    }

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule())
            .configure(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    
    // Schema Cache per (Registry URL + Schema ID)
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();

    /**
     * Deserialize binary payload into a JSON string.
     * Decides whether to use a Confluent schema registry ID or a custom schema.
     */
    public String deserialize(byte[] payload, String registryUrl, String customSchemaText) {
        if (payload == null || payload.length == 0) {
            return "";
        }

        try {
            // Case 1: Custom schema was pasted manually by the user
            if (customSchemaText != null && !customSchemaText.trim().isEmpty()) {
                log.debug("Using custom manual schema for deserialization");
                Schema schema = new Schema.Parser().parse(customSchemaText);
                return decodeAvroWithSchema(payload, 0, payload.length, schema);
            }

            // Case 2: Confluent Schema Registry format (Magic byte 0x00 + 4-byte schema ID)
            if (registryUrl != null && !registryUrl.trim().isEmpty() && payload.length >= 5 && payload[0] == 0) {
                log.debug("Using Schema Registry for deserialization");
                ByteBuffer buffer = ByteBuffer.wrap(payload);
                buffer.get(); // Skip magic byte
                int schemaId = buffer.getInt(); // Read 4-byte schema ID
                
                Schema schema = fetchSchema(registryUrl, schemaId);
                return decodeAvroWithSchema(payload, 5, payload.length - 5, schema);
            }

            // Fallback: If no schema matches, return UTF-8 string or HEX if not printable
            return tryStringOrHex(payload);

        } catch (Exception e) {
            log.warn("Avro deserialization failed, falling back to raw string. Error: {}", e.getMessage());
            return "/* [Deserialization Failed: " + e.getMessage() + "] */\n" + tryStringOrHex(payload);
        }
    }

    /**
     * Decode Avro binary data using a parsed schema and convert it to pretty JSON.
     */
    private String decodeAvroWithSchema(byte[] data, int offset, int length, Schema schema) throws Exception {
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema, schema, GENERIC_DATA);
        ByteArrayInputStream in = new ByteArrayInputStream(data, offset, length);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        
        GenericRecord record = reader.read(null, decoder);
        Object jsonObject = convertAvroObject(record);
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
    }

    /**
     * Fetch schema from registry, using cache to prevent excessive network requests.
     */
    private Schema fetchSchema(String registryUrl, int schemaId) {
        String cleanUrl = registryUrl.endsWith("/") ? registryUrl.substring(0, registryUrl.length() - 1) : registryUrl;
        String cacheKey = cleanUrl + "#" + schemaId;

        return schemaCache.computeIfAbsent(cacheKey, key -> {
            String endpoint = cleanUrl + "/schemas/ids/" + schemaId;
            log.info("Fetching Avro schema from endpoint: {}", endpoint);
            try {
                // Confluent Schema Registry returns a JSON object: {"schema": "..."}
                Map<?, ?> response = restTemplate.getForObject(endpoint, Map.class);
                if (response != null && response.containsKey("schema")) {
                    String schemaJson = (String) response.get("schema");
                    return new Schema.Parser().parse(schemaJson);
                }
                throw new RuntimeException("Schema ID not found in registry response");
            } catch (Exception e) {
                log.error("Failed to fetch schema " + schemaId + " from registry", e);
                throw new RuntimeException("Registry connection failed: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Recursive utility to convert Avro structures (GenericRecord, Arrays, Enums) 
     * into pure Java Objects (Maps, Lists) that Jackson ObjectMapper can format cleanly.
     */
    private Object convertAvroObject(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof GenericRecord record) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (Schema.Field field : record.getSchema().getFields()) {
                map.put(field.name(), convertAvroObject(record.get(field.name())));
            }
            return map;
        }

        if (obj instanceof Collection<?> collection) {
            List<Object> list = new ArrayList<>();
            for (Object item : collection) {
                list.add(convertAvroObject(item));
            }
            return list;
        }

        if (obj instanceof GenericEnumSymbol enumSymbol) {
            return enumSymbol.toString();
        }

        if (obj instanceof ByteBuffer byteBuffer) {
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            
            // Try to see if the bytes themselves are already an ASCII hex string
            try {
                String str = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                if (str.length() > 0 && str.length() % 2 == 0 && str.matches("^[0-9a-fA-F]+$")) {
                    return "0x" + str;
                }
            } catch (Exception ignored) {
            }
            
            // Otherwise, convert raw bytes to hex string starting with 0x
            StringBuilder sb = new StringBuilder("0x");
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }

        if (obj instanceof CharSequence) {
            return obj.toString();
        }

        return obj;
    }

    /**
     * Tries to decode bytes as standard UTF-8 string. 
     * If there are non-printable characters, returns a hex representation.
     */
    public String tryStringOrHex(byte[] payload) {
        if (payload == null) return "";
        
        // Check if printable UTF-8
        boolean isPrintable = true;
        for (byte b : payload) {
            if (b < 32 && b != 9 && b != 10 && b != 13) {
                // Non-printable control char (excluding tab, newline, carriage return)
                isPrintable = false;
                break;
            }
        }

        if (isPrintable) {
            return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        } else {
            // Return Hex string
            StringBuilder sb = new StringBuilder();
            sb.append("0x");
            for (byte b : payload) {
                sb.append(String.format("%02X", b));
            }
            return sb.toString();
        }
    }
}
