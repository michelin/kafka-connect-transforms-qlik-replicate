package com.michelin.kafka.connect.transforms.qlik.replicate;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.michelin.kafka.connect.transforms.qlik.replicate.ChangeEvent.HEADER_SCHEMA;
import static com.michelin.kafka.connect.transforms.qlik.replicate.ChangeEvent.Headers.*;
import static com.michelin.kafka.connect.transforms.qlik.replicate.ChangeEvent.Message.*;
import static org.assertj.core.api.Assertions.assertThat;

public class ExtractNewRecordStateTest {

    private final ExtractNewRecordState<SourceRecord> smt = new ExtractNewRecordState();


    public static Schema TABLE_SCHEMA = SchemaBuilder.struct()
        .field("id", SchemaBuilder.int8())
        .field("name", SchemaBuilder.string())
        .optional()
        .build();

    public static Struct DEFAULT_DATA = new Struct(TABLE_SCHEMA)
        .put("id", (byte) 1)
        .put("name", "foo");

    public static Schema UPDATED_TABLE_SCHEMA = SchemaBuilder.struct()
        .field("id", SchemaBuilder.int8())
        .field("name", SchemaBuilder.string())
        .field("age", SchemaBuilder.int32())
        .optional()
        .build();

    public static Struct UPDATED_DATA = new Struct(UPDATED_TABLE_SCHEMA)
        .put("id", (byte) 1)
        .put("name", "bar")
        .put("age", 0);

    private Schema changeEventSchema = SchemaBuilder.struct()
        .field(DATA, TABLE_SCHEMA)
        .field(BEFORE_DATA, TABLE_SCHEMA)
        .field(HEADERS, HEADER_SCHEMA)
        .build();

    private Struct insertChangeEvent = new Struct(changeEventSchema)
        .put(BEFORE_DATA, null)
        .put(DATA, DEFAULT_DATA)
        .put(HEADERS, headers("INSERT"));

    private Struct deleteChangeEvent = new Struct(changeEventSchema)
        .put(BEFORE_DATA, null)
        .put(DATA, DEFAULT_DATA)
        .put(HEADERS, headers("DELETE"));

    @AfterEach
    public void close() {
        smt.close();
    }

    @Test
    public void shouldUnWrapEnvelopeWhenPresent() {
        Schema eventWithEnvelopeSchema = SchemaBuilder.struct()
            .field(ChangeEvent.Envelope.MAGIC, SchemaBuilder.string())
            .field(ChangeEvent.Envelope.TYPE, SchemaBuilder.string())
            .field(ChangeEvent.Envelope.HEADERS, SchemaBuilder.string().optional())
            .field(ChangeEvent.Envelope.MESSAGE_SCHEMA_ID, SchemaBuilder.string().optional())
            .field(ChangeEvent.Envelope.MESSAGE_SCHEMA, SchemaBuilder.string().optional())
            .field(ChangeEvent.Envelope.MESSAGE, changeEventSchema)
            .build();

        Struct eventWithEnvelope = new Struct(eventWithEnvelopeSchema)
            .put(ChangeEvent.Envelope.MAGIC, "atMSG")
            .put(ChangeEvent.Envelope.TYPE, "DT")
            .put(ChangeEvent.Envelope.HEADERS, null)
            .put(ChangeEvent.Envelope.MESSAGE_SCHEMA_ID, null)
            .put(ChangeEvent.Envelope.MESSAGE_SCHEMA, null)
            .put(ChangeEvent.Envelope.MESSAGE, insertChangeEvent);

        smt.configure(Collections.emptyMap());

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, eventWithEnvelopeSchema, eventWithEnvelope
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.valueSchema().field(ChangeEvent.Envelope.MAGIC)).isNull();
        assertThat(((Struct) transformedRecord.value()).getInt8("id")).isEqualTo((byte) 1);


    }

    @Test
    public void shouldSkipTombstones() {
        smt.configure(Collections.emptyMap());

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, null
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord).isNull();
    }

    @Test
    public void shouldApplyDeleteByDefault() {
        smt.configure(Collections.emptyMap());

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, deleteChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.value()).isNull();
        assertThat(transformedRecord.keySchema()).isEqualTo(record.keySchema());
        assertThat(transformedRecord.valueSchema()).isNull();

    }

    @Test
    public void shouldApplyDeleteWhenConfigured() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.APPLY.name()));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, deleteChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.value()).isNull();
        assertThat(transformedRecord.keySchema()).isEqualTo(record.keySchema());
        assertThat(transformedRecord.valueSchema()).isNull();
    }

    @Test
    public void shouldApplyDeleteWhenConfigureAndNoSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.APPLY.name());
        smt.configure(properties);
        Map<String, Object> headersWithoutSchema = new HashMap<>();
        headersWithoutSchema.put(OPERATION, "DELETE");
        headersWithoutSchema.put(CHANGE_SEQUENCE, "0");
        headersWithoutSchema.put(TIMESTAMP, Instant.now().toString());
        headersWithoutSchema.put(STREAM_POSITION, "0");
        headersWithoutSchema.put(TRANSACTION_ID, "0");
        headersWithoutSchema.put(CHANGE_MASK, "\u0003".getBytes());
        headersWithoutSchema.put(COLUMN_MASK, "\u0003".getBytes());
        headersWithoutSchema.put(TRANSACTION_EVENT_COUNTER, 0);
        headersWithoutSchema.put(TRANSACTION_LAST_EVENT, false);

        Map<String, Object> dataWithoutSchema = new HashMap<>();
        dataWithoutSchema.put("id", (byte) 1);
        dataWithoutSchema.put("name", "foo");

        Map<String, Object> deleteChangeEventWithoutSchema = new HashMap<>();
        deleteChangeEventWithoutSchema.put(BEFORE_DATA, null);
        deleteChangeEventWithoutSchema.put(DATA, dataWithoutSchema);
        deleteChangeEventWithoutSchema.put(HEADERS, headersWithoutSchema);

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, null, deleteChangeEventWithoutSchema
        );

        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.value()).isNull();
        assertThat(transformedRecord.keySchema()).isNull();
        assertThat(transformedRecord.valueSchema()).isNull();
    }


    @Test
    public void shouldSkipDeleteWhenConfigured() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.SKIP.name()));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, deleteChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord).isNull();
    }

    @Test
    public void shouldFlagRecordAsDeletedWhenSoftDeleteConfiguredAndDeleteEvent() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.SOFT.name()));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, deleteChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.valueSchema().field(ExtractNewRecordState.SOFT_DELETE_FIELD)).isNotNull();
        assertThat(((Struct) transformedRecord.value()).getString(ExtractNewRecordState.SOFT_DELETE_FIELD)).isEqualTo("true");
    }

    @Test
    public void shouldFlagRecordAsNotDeletedWhenSoftDeleteConfiguredAndInsertOrUpdateEvent() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.SOFT.name()));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, insertChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.valueSchema().field(ExtractNewRecordState.SOFT_DELETE_FIELD)).isNotNull();
        assertThat(((Struct) transformedRecord.value()).getString(ExtractNewRecordState.SOFT_DELETE_FIELD)).isEqualTo("false");
    }

    @Test
    public void shouldThrowConfigExceptionWhenUnknownDeleteMode() {
        Exception thrown = Assertions.assertThrows(ConfigException.class, () -> {
            smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.DELETE_MODE, "foo"));
        });
        assertThat(thrown.getMessage()).containsIgnoringCase("Valid values are (APPLY, SKIP, SOFT");
    }

    @Test
    public void shouldUpdateSchemaWhenUpdateEvent() {
        smt.configure(Collections.emptyMap());

        Schema updateChangeEventSchema = SchemaBuilder.struct()
            .field(DATA, UPDATED_TABLE_SCHEMA)
            .field(BEFORE_DATA, TABLE_SCHEMA)
            .field(HEADERS, HEADER_SCHEMA)
            .build();

        Struct updateChangeEvent = new Struct(updateChangeEventSchema)
            .put(BEFORE_DATA, DEFAULT_DATA)
            .put(DATA, UPDATED_DATA)
            .put(HEADERS, headers("UPDATE"));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, updateChangeEventSchema, updateChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);

        assertThat(transformedRecord.value()).isNotNull();
        assertThat(transformedRecord.valueSchema().field("age")).isNotNull();
        assertThat(((Struct) transformedRecord.value()).getInt32("age")).isEqualTo(0);
    }

    @Test
    public void shouldThrowConfigExceptionWhenAddingUnknownField() {
        Exception thrown = Assertions.assertThrows(ConfigException.class, () -> {
            smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.ADD_FIELDS, "foo"));
        });
        assertThat(thrown.getMessage()).containsIgnoringCase("Valid values are (operation, changeSequence, timestamp, streamPosition, transactionId, changeMask, columnMask, transactionEventCounter, transactionLastEvent)");
    }

    @Test
    public void shouldAddFieldWhenConfigured() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.ADD_FIELDS, "operation,transactionEventCounter"));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, insertChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.valueSchema().field("__operation")).isNotNull();
        assertThat(transformedRecord.valueSchema().field("__transactionEventCounter")).isNotNull();
        assertThat(((Struct) transformedRecord.value()).getString("__operation")).isEqualTo("INSERT");
        assertThat(((Struct) transformedRecord.value()).getString("__transactionEventCounter")).isEqualTo("0");
    }

    @Test
    public void shouldAddHeaderWhenConfigured() {
        smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.ADD_HEADERS, "operation,transactionEventCounter"));

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, changeEventSchema, insertChangeEvent
        );
        SourceRecord transformedRecord = smt.apply(record);
        assertThat(transformedRecord.headers().lastWithName("__operation")).isNotNull();
        assertThat(transformedRecord.headers().lastWithName("__operation").value()).isEqualTo("INSERT");
        assertThat(transformedRecord.headers().lastWithName("__transactionEventCounter")).isNotNull();
        assertThat(transformedRecord.headers().lastWithName("__transactionEventCounter").value()).isEqualTo(0);
    }

    @Test
    public void shouldThrowConfigExceptionWhenAddingUnknownHeader() {
        Exception thrown = Assertions.assertThrows(ConfigException.class, () -> {
            smt.configure(Collections.singletonMap(ExtractNewRecordStateConfig.ADD_HEADERS, "foo"));
        });
        assertThat(thrown.getMessage()).containsIgnoringCase("Valid values are (operation, changeSequence, timestamp, streamPosition, transactionId, changeMask, columnMask, transactionEventCounter, transactionLastEvent)");
    }

    @Test
    public void shouldAddMetadataWhenNoSchema() {
        Map<String, String> properties = new HashMap<>();
        properties.put(ExtractNewRecordStateConfig.DELETE_MODE, DeleteMode.SOFT.name());
        properties.put(ExtractNewRecordStateConfig.ADD_FIELDS, "operation,transactionEventCounter");
        properties.put(ExtractNewRecordStateConfig.ADD_HEADERS, "transactionLastEvent");
        smt.configure(properties);
        Map<String, Object> headersWithoutSchema = new HashMap<>();
        headersWithoutSchema.put(OPERATION, "INSERT");
        headersWithoutSchema.put(CHANGE_SEQUENCE, "0");
        headersWithoutSchema.put(TIMESTAMP, Instant.now().toString());
        headersWithoutSchema.put(STREAM_POSITION, "0");
        headersWithoutSchema.put(TRANSACTION_ID, "0");
        headersWithoutSchema.put(CHANGE_MASK, "\u0003".getBytes());
        headersWithoutSchema.put(COLUMN_MASK, "\u0003".getBytes());
        headersWithoutSchema.put(TRANSACTION_EVENT_COUNTER, 0);
        headersWithoutSchema.put(TRANSACTION_LAST_EVENT, false);

        Map<String, Object> dataWithoutSchema = new HashMap<>();
        dataWithoutSchema.put("id", (byte) 1);
        dataWithoutSchema.put("name", "foo");

        Map<String, Object> insertChangeEventWithoutSchema = new HashMap<>();
        insertChangeEventWithoutSchema.put(BEFORE_DATA, null);
        insertChangeEventWithoutSchema.put(DATA, dataWithoutSchema);
        insertChangeEventWithoutSchema.put(HEADERS, headersWithoutSchema);

        SourceRecord record = new SourceRecord(
            null, null, "test", 0, null, insertChangeEventWithoutSchema
        );

        SourceRecord transformedRecord = smt.apply(record);

        assertThat(transformedRecord.keySchema()).isNull();
        assertThat(transformedRecord.valueSchema()).isNull();

        Map<String, Object> values = (Map<String, Object>) transformedRecord.value();
        assertThat(values.containsKey("id")).isTrue();
        assertThat((byte) values.get("id")).isEqualTo((byte) 1);
        assertThat(values.containsKey("name")).isTrue();
        assertThat((String) values.get("name")).isEqualToIgnoringCase("foo");
        assertThat(values.containsKey("__deleted")).isTrue();
        assertThat((String) values.get("__deleted")).isEqualToIgnoringCase("false");
        assertThat(values.containsKey("__operation")).isTrue();
        assertThat((String) values.get("__operation")).isEqualToIgnoringCase("INSERT");
        assertThat(transformedRecord.headers().lastWithName("__transactionLastEvent")).isNotNull();
        assertThat(transformedRecord.headers().lastWithName("__transactionLastEvent").value()).isEqualTo(false);
    }

    public static Struct headers(String operation) {
        return new Struct(HEADER_SCHEMA)
            .put(OPERATION, operation)
            .put(CHANGE_SEQUENCE, "0")
            .put(TIMESTAMP, Instant.now().toString())
            .put(STREAM_POSITION, "0")
            .put(TRANSACTION_ID, "0")
            .put(CHANGE_MASK, "\u0003".getBytes())
            .put(COLUMN_MASK, "\u0003".getBytes())
            .put(TRANSACTION_EVENT_COUNTER, 0)
            .put(TRANSACTION_LAST_EVENT, false);
    }

}
