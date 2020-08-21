package com.michelin.kafka.connect.transforms.qlik.replicate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;

import static com.michelin.kafka.connect.transforms.qlik.replicate.ChangeEvent.Headers.*;

public class ChangeEvent {

    public class Envelope {
        public static final String MAGIC = "magic";
        public static final String TYPE = "type";
        public static final String HEADERS = "headers";
        public static final String MESSAGE_SCHEMA_ID = "messageSchemaId";
        public static final String MESSAGE_SCHEMA = "messageSchema";
        public static final String MESSAGE = "message";
    }

    public class Message {
        public static final String BEFORE_DATA = "beforeData";
        public static final String DATA = "data";
        public static final String HEADERS = "headers";
    }


    public class Headers {
        public static final String OPERATION = "operation";
        public static final String CHANGE_SEQUENCE = "changeSequence";
        public static final String TIMESTAMP = "timestamp";
        public static final String STREAM_POSITION = "streamPosition";
        public static final String TRANSACTION_ID = "transactionId";
        public static final String CHANGE_MASK = "changeMask";
        public static final String COLUMN_MASK = "columnMask";
        public static final String TRANSACTION_EVENT_COUNTER = "transactionEventCounter";
        public static final String TRANSACTION_LAST_EVENT = "transactionLastEvent";
    }

    public static List<String> ALL_HEADER_NAMES = Arrays.asList(
        Headers.OPERATION,
        Headers.CHANGE_SEQUENCE,
        Headers.TIMESTAMP,
        Headers.STREAM_POSITION,
        Headers.TRANSACTION_ID,
        Headers.CHANGE_MASK,
        Headers.COLUMN_MASK,
        Headers.TRANSACTION_EVENT_COUNTER,
        Headers.TRANSACTION_LAST_EVENT
    );

    public static Schema HEADER_SCHEMA = SchemaBuilder.struct()
        .field(OPERATION, SchemaBuilder.string())
        .field(CHANGE_SEQUENCE, SchemaBuilder.string())
        .field(TIMESTAMP, SchemaBuilder.string())
        .field(STREAM_POSITION, SchemaBuilder.string())
        .field(TRANSACTION_ID, SchemaBuilder.string())
        .field(CHANGE_MASK, SchemaBuilder.bytes())
        .field(COLUMN_MASK, SchemaBuilder.bytes())
        .field(TRANSACTION_EVENT_COUNTER, SchemaBuilder.int32())
        .field(TRANSACTION_LAST_EVENT, SchemaBuilder.bool())
        .build();

}
