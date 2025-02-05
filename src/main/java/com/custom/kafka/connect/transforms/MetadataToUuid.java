package com.custom.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetadataToUuid<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String FIELD_CONFIG = "field.name";
    private static final ObjectMapper mapper = new ObjectMapper();
    private String fieldName;

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            throw new RuntimeException("Record value (metadata) cannot be null");
        }

        Map<String, Object> metadata;
        try {
            if (record.value() instanceof Map) {
                metadata = (Map<String, Object>) record.value();
            } else {
                metadata = mapper.readValue(record.value().toString(), Map.class);
            }

            // Verificar campos requeridos
            if (!metadata.containsKey("kafka_key") || !metadata.containsKey("kafka_position") ||
                    !metadata.containsKey("kafka_timestamp") || !metadata.containsKey("kafka_topic")) {
                throw new RuntimeException("Missing required metadata fields");
            }

            // Extraer los campos necesarios
            String kafkaKey = metadata.get("kafka_key").toString();
            String kafkaPosition = metadata.get("kafka_position").toString();
            String kafkaTimestamp = metadata.get("kafka_timestamp").toString();
            String kafkaTopic = metadata.get("kafka_topic").toString();

            // Combinar campos
            String combined = String.join("", kafkaKey, kafkaPosition, kafkaTimestamp, kafkaTopic);

            // Generar UUID
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
        return new ConfigDef()
                .define(FIELD_CONFIG, Type.STRING, "key", Importance.HIGH,
                        "Field name for the UUID");
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {
        fieldName = configs.get(FIELD_CONFIG).toString();
    }
}
