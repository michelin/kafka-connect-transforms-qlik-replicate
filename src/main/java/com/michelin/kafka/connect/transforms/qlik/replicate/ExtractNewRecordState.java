package com.michelin.kafka.connect.transforms.qlik.replicate;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExtractNewRecordState<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractNewRecordState.class);

    public static final String METADATA_FIELD_PREFIX = "__";
    public static final String SOFT_DELETE_FIELD = METADATA_FIELD_PREFIX + "deleted";

    private DeleteMode deleteMode;
    private List<String> additionalFields = new ArrayList<>();
    private List<String> additionalHeaders = new ArrayList<>();
    private List<InsertField> additionalFieldsHandlers = new ArrayList<>();
    private ExtractField<R> envelopeHandler = new ExtractField.Value<>();
    private ExtractField<R> dataHandler = new ExtractField.Value<>();
    private ExtractField<R> beforeDataHandler = new ExtractField.Value<>();
    private InsertField<R> softDeleteForDeletedRecordHandler = new InsertField.Value<>();
    private InsertField<R> softDeleteForInsertOrUpdateRecordHandler = new InsertField.Value<>();

    @Override
    public void configure(Map<String, ?> properties) {
        ExtractNewRecordStateConfig config = new ExtractNewRecordStateConfig(properties);

        deleteMode = DeleteMode.valueOf(config.getString(ExtractNewRecordStateConfig.DELETE_MODE).toUpperCase());
        dataHandler.configure(Collections.singletonMap("field", ChangeEvent.Message.DATA));
        beforeDataHandler.configure(Collections.singletonMap("field", ChangeEvent.Message.BEFORE_DATA));
        additionalFields = config.getList(ExtractNewRecordStateConfig.ADD_FIELDS);
        additionalHeaders = config.getList(ExtractNewRecordStateConfig.ADD_HEADERS);

        if (deleteMode == DeleteMode.SOFT) {
            Map<String, String> softDeleteHandlerConfig = new HashMap<>();
            softDeleteHandlerConfig.put("static.field", SOFT_DELETE_FIELD);
            softDeleteHandlerConfig.put("static.value", "true");
            softDeleteForDeletedRecordHandler.configure(softDeleteHandlerConfig);

            softDeleteHandlerConfig.put("static.value", "false");
            softDeleteForInsertOrUpdateRecordHandler.configure(softDeleteHandlerConfig);
        }

        envelopeHandler.configure(Collections.singletonMap("field", ChangeEvent.Envelope.MESSAGE));
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            LOG.info("Skipping record {} because is a tombstone", record.key());
            return null;
        }

        if (hasEnvelope(record)) {
            LOG.debug("Extracting Qlik Replicate envelope");
            record = envelopeHandler.apply(record);
        }

        R data = dataHandler.apply(record);

        String operation = (String) getChangeEventHeader(record, ChangeEvent.Headers.OPERATION);
        if (operation.equalsIgnoreCase("DELETE")) {
            LOG.info("Record {} deleted, applying strategy: {}", record.key(), deleteMode);
            switch (deleteMode) {
                case SKIP:
                    return null;
                case SOFT:
                    data = softDeleteForDeletedRecordHandler.apply(data);
                    break;
                default:
                    data = data.newRecord(
                        data.topic(),
                        data.kafkaPartition(),
                        data.keySchema(),
                        data.key(),
                        null,
                        null,
                        data.timestamp(),
                        data.headers()
                    );
                    break;
            }
        } else {
            if (deleteMode == DeleteMode.SOFT) {
                data = softDeleteForInsertOrUpdateRecordHandler.apply(data);
            }
        }
        if (!additionalFields.isEmpty()) {
            for (String field : additionalFields) {
                String fieldName = METADATA_FIELD_PREFIX + field;
                String fieldValue = getChangeEventHeader(record, field).toString();
                Map<String, String> fieldHandlerConfig = new HashMap<>();
                fieldHandlerConfig.put("static.field", fieldName);
                fieldHandlerConfig.put("static.value", fieldValue);
                InsertField<R> fieldHandler = new InsertField.Value<>();
                fieldHandler.configure(fieldHandlerConfig);
                additionalFieldsHandlers.add(fieldHandler);
                LOG.debug("Adding field {}: {}", record.key(), fieldName, fieldValue);
                data = fieldHandler.apply(data);
            }
        }
        if (!additionalHeaders.isEmpty()) {
            for (String header : additionalHeaders) {
                String headerName = METADATA_FIELD_PREFIX + header;
                Object headerValue = getChangeEventHeader(record, header);
                LOG.debug("Adding header {}: {}", record.key(), headerName, headerValue);
                data.headers().add(
                    headerName,
                    headerValue,
                    ChangeEvent.HEADER_SCHEMA.field(header).schema()
                );
            }
        }

        return data;

    }

    private Object getChangeEventHeader(R record, String name) {
        if (record.value() instanceof Struct) {
            return ((Struct) record.value()).getStruct(ChangeEvent.Message.HEADERS).get(name);
        }
        return ((Map) ((Map) record.value()).get(ChangeEvent.Message.HEADERS)).get(name);
    }

    private boolean hasEnvelope(R record) {
        if (record.value() instanceof Struct) {
            return record.valueSchema().field(ChangeEvent.Envelope.MAGIC) != null;
        }
        return ((Map) record.value()).get(ChangeEvent.Envelope.MAGIC) != null;
    }

    @Override
    public ConfigDef config() {
        return ExtractNewRecordStateConfig.CONFIG_DEF;
    }

    @Override
    public void close() {
        dataHandler.close();
        beforeDataHandler.close();
        softDeleteForDeletedRecordHandler.close();
        softDeleteForInsertOrUpdateRecordHandler.close();
        envelopeHandler.close();
        for (InsertField handler : additionalFieldsHandlers) {
            handler.close();
        }

    }
}
