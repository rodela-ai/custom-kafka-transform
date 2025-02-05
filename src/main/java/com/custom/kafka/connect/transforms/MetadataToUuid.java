package com.custom.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

public class MetadataToUuid<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public R apply(R record) {
        try {
            JsonNode rootNode;
            if (record.value() instanceof String) {
                rootNode = mapper.readTree((String) record.value());
            } else if (record.value() instanceof Map) {
                rootNode = mapper.valueToTree(record.value());
            } else {
                rootNode = mapper.valueToTree(record.value());
            }

            // Navegar a la sección metadata
            JsonNode metadataNode = rootNode.path("metadata");
            if (metadataNode.isMissingNode()) {
                throw new RuntimeException("Metadata section not found in message");
            }

            // Extraer los campos necesarios
            String kafkaKey = metadataNode.path("kafka_key").asText();
            String kafkaPosition = metadataNode.path("kafka_position").asText();
            String kafkaTimestamp = metadataNode.path("kafka_timestamp").asText();
            String kafkaTopic = metadataNode.path("kafka_topic").asText();

            if (kafkaKey.isEmpty() || kafkaPosition.isEmpty() ||
                    kafkaTimestamp.isEmpty() || kafkaTopic.isEmpty()) {
                throw new RuntimeException("Missing required metadata fields");
            }

            // Combinar campos para generar UUID
            String combined = String.join("", kafkaKey, kafkaPosition, kafkaTimestamp, kafkaTopic);
            UUID uuid = generateUUID(combined);

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    uuid.toString(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );

        } catch (Exception e) {
            throw new RuntimeException("Error processing metadata: " + e.getMessage(), e);
        }
    }

    private UUID generateUUID(String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);

        long most = crc32.getValue();
        crc32.reset();
        crc32.update((input + "salt").getBytes(StandardCharsets.UTF_8));
        long least = crc32.getValue();

        return new UUID(most, least);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}