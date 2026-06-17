package com.example.kafkaflow.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class AvroDeserializerTest {

    @Test
    public void testLogicalTypesSerialization() throws Exception {
        String schemaStr = "{\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"TestRecord\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"dec\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 4, \"scale\": 2}},\n" +
                "    {\"name\": \"uuid\", \"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"}},\n" +
                "    {\"name\": \"dt\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}},\n" +
                "    {\"name\": \"ts\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}\n" +
                "  ]\n" +
                "}";

        Schema schema = new Schema.Parser().parse(schemaStr);

        GenericData genericData = new GenericData();
        genericData.addLogicalTypeConversion(new org.apache.avro.Conversions.DecimalConversion());
        genericData.addLogicalTypeConversion(new org.apache.avro.Conversions.UUIDConversion());
        genericData.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
        genericData.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());

        GenericRecord record = new GenericRecordBuilder(schema)
                .set("dec", new BigDecimal("12.34"))
                .set("uuid", UUID.fromString("123e4567-e89b-12d3-a456-426614174000"))
                .set("dt", LocalDate.of(2026, 6, 17))
                .set("ts", Instant.parse("2026-06-17T09:02:27Z"))
                .build();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema, genericData);
        writer.write(record, encoder);
        encoder.flush();
        byte[] serializedBytes = out.toByteArray();

        AvroDeserializer deserializer = new AvroDeserializer();
        String json = deserializer.deserialize(serializedBytes, null, schemaStr);
        System.out.println("Deserialized JSON:");
        System.out.println(json);
        
        assertTrue(json.contains("12.34"), "JSON should contain decimal value");
        assertTrue(json.contains("123e4567-e89b-12d3-a456-426614174000"), "JSON should contain UUID");
        assertTrue(json.contains("2026-06-17"), "JSON should contain date");
    }
}
