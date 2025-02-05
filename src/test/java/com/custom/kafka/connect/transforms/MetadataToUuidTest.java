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

        // Crear un map simulando los metadatos
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("kafka_key", "1738766601");
        metadata.put("kafka_position", 1);
        metadata.put("kafka_timestamp", 1738766601592L);
        metadata.put("kafka_topic", "operation-output");

        // Crear las estructuras necesarias para el SourceRecord
        HashMap<String, Object> sourcePartition = new HashMap<>();
        HashMap<String, Object> sourceOffset = new HashMap<>();

        // Crear el SourceRecord con los metadatos
        SourceRecord record = new SourceRecord(
                sourcePartition,
                sourceOffset,
                "test-topic",
                null,  // key schema
                null,  // key
                null,  // value schema
                metadata  // value (metadata map)
        );

        // Configurar la transformación
        Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "key");
        transform.configure(configs);

        // Aplicar la transformación
        SourceRecord transformed = transform.apply(record);

        // Verificaciones
        assertNotNull("La transformación no debería devolver null", transformed);
        assertNotNull("La key no debería ser null", transformed.key());

        String uuid = transformed.key().toString();

        // Verificar que es un UUID válido
        assertEquals("El UUID debería tener 36 caracteres", 36, uuid.length());
        assertTrue("El UUID debería contener guiones", uuid.contains("-"));

        // Verificar que la transformación es determinista
        SourceRecord transformed2 = transform.apply(record);
        assertEquals("La misma entrada debería producir el mismo UUID",
                transformed.key(), transformed2.key());
    }

    @Test
    public void testNullMetadata() {
        MetadataToUuid<SourceRecord> transform = new MetadataToUuid<>();

        // Configurar la transformación
        Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "key");
        transform.configure(configs);

        // Crear un SourceRecord con valor null
        SourceRecord record = new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                "test-topic",
                null,
                null
        );

        try {
            transform.apply(record);
            fail("Debería haber lanzado una excepción con metadata null");
        } catch (RuntimeException e) {
            assertTrue("El mensaje de error debería mencionar metadata",
                    e.getMessage().contains("metadata"));
        }
    }
}