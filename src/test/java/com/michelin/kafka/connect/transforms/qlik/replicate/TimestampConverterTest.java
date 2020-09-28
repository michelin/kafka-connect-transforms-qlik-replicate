package com.michelin.kafka.connect.transforms.qlik.replicate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TimestampConverterTest {

    private final TimestampConverter.Value<SourceRecord> smt = new TimestampConverter.Value<>();

    @Test
    public void should() {
        Map<String, String> properties = new HashMap<>();
        properties.put("field", "timestamp");
        properties.put("target.type", "unix");

        smt.configure(properties);

        Schema schemaWithTimestampMicros = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("timestamp", SchemaBuilder.int64().parameter("type", "long").parameter("logicalType", "timestamp-micros"))
            .optional()
            .build();

        Struct recordWithTimestampMicros = new Struct(schemaWithTimestampMicros)
            .put("id", (byte) 1)
            .put("timestamp", 1126196428000000L);

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, schemaWithTimestampMicros, recordWithTimestampMicros
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(((Struct) transformedRecord.value()).get("timestamp")).isEqualTo(1126196428000L);
    }
}
