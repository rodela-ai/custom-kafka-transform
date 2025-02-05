// MetadataToUuidTest.java
package com.custom.kafka.connect.transforms;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;

public class MetadataToUuidTest {

    @Test
    public void testUuidGeneration() {
        MetadataToUuid<SourceRecord> transform = new MetadataToUuid<>();

        // Crear un mensaje Activity Streams completo con metadata
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("kafka_key", "1738766601");
        metadata.put("kafka_position", 1);
        metadata.put("kafka_timestamp", 1738766601592L);
        metadata.put("kafka_topic", "operation-output");

        Map<String, Object> activityStream = new HashMap<>();
        activityStream.put("@context", "https://www.w3.org/ns/activitystreams");
        activityStream.put("metadata", metadata);

        // Crear el SourceRecord
        SourceRecord record = new SourceRecord(
                new HashMap<>(),  // sourcePartition
                new HashMap<>(),  // sourceOffset
                "test-topic",     // topic
                null,            // keySchema
                null,            // key
                null,            // valueSchema
                activityStream   // value
        );

        // Aplicar la transformación
        SourceRecord transformed = transform.apply(record);

        // Verificaciones
        assertNotNull("La transformación no debería devolver null", transformed);
        assertNotNull("La key no debería ser null", transformed.key());

        String uuid = transformed.key().toString();

        // Verificar formato UUID
        assertEquals("El UUID debería tener 36 caracteres", 36, uuid.length());
        assertTrue("El UUID debería contener guiones", uuid.contains("-"));

        // Verificar determinismo
        SourceRecord transformed2 = transform.apply(record);
        assertEquals("La misma entrada debería producir el mismo UUID",
                transformed.key(), transformed2.key());
    }

    @Test(expected = RuntimeException.class)
    public void testMissingMetadata() {
        MetadataToUuid<SourceRecord> transform = new MetadataToUuid<>();

        // Crear mensaje sin metadata
        Map<String, Object> activityStream = new HashMap<>();
        activityStream.put("@context", "https://www.w3.org/ns/activitystreams");

        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "test-topic",
                null,
                null,
                null,
                activityStream
        );

        transform.apply(record);  // Debería lanzar RuntimeException
    }
}