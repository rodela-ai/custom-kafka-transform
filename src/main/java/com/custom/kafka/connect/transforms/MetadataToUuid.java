package com.custom.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

public class MetadataToUuid<R extends ConnectRecord<R>> implements Transformation<R> {
    
    @Override
    public R apply(R record) {
        String[] parts = record.key().toString().split(",");
        
        // Combine all metadata fields
        String combined = String.join("", parts);
        
        // Generate deterministic UUID using the combined string
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
    }
    
    private UUID generateUUID(String input) {
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        CRC32 crc32 = new CRC32();
        crc32.update(bytes);
        
        // Use CRC32 to generate deterministic long values
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
