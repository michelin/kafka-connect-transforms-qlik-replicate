package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TimestampMicros {
    public static final String LOGICAL_NAME = "org.apache.kafka.connect.data.TimestampMicros";
    public static final Schema SCHEMA = builder().schema();

    public TimestampMicros() {
    }

    public static SchemaBuilder builder() {
        return SchemaBuilder.int64().name("org.apache.kafka.connect.data.TimestampMicros").version(1);
    }

    public static long fromLogical(Schema schema, java.util.Date value) {
        if (!"org.apache.kafka.connect.data.TimestampMicros".equals(schema.name())) {
            throw new DataException("Requested conversion of TimestampMicros object but the schema does not match.");
        } else {
            return ChronoUnit.MILLIS.between(Instant.EPOCH, value.toInstant());
        }
    }

    public static java.util.Date toLogical(Schema schema, long value) {
        if (!"org.apache.kafka.connect.data.TimestampMicros".equals(schema.name())) {
            throw new DataException("Requested conversion of TimestampMicros object but the schema does not match.");
        } else {
            return Date.from(Instant.EPOCH.plus(value, ChronoUnit.MICROS));
        }
    }
}
