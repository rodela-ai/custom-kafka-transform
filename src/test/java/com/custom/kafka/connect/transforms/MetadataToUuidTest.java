package com.custom.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashMap;

public class MetadataToUuidTest {
    
    @Test
    public void testUuidGeneration() {
        MetadataToUuid<SourceRecord> transform = new MetadataToUuid<>();
        
        // Create a test record
        String testKey = "1738766601,1,1738766601592,operation-output";
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();
        
        SourceRecord record = new SourceRecord(
            sourcePartition,
            sourceOffset,
            "test-topic",
            null,
            testKey,
            null,
            null
        );
        
        // Apply transformation
        SourceRecord transformed = transform.apply(record);
        
        // Verify the result is a valid UUID
        String uuid = transformed.key().toString();
        assertNotNull(uuid);
        assertEquals(36, uuid.length());  // UUID string length
        
        // Test deterministic behavior
        SourceRecord transformed2 = transform.apply(record);
        assertEquals(transformed.key(), transformed2.key());
    }
}
