package com.michelin.kafka.connect.transforms.qlik.replicate;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ExtractNewRecordStateConfig extends AbstractConfig {

    public static final String DELETE_MODE = "delete.mode";
    public static final DeleteMode DELETE_MODE_DEFAULT = DeleteMode.APPLY;
    public static final String DELETE_MODE_DOC = "How to handle delete records. Options are: "
        + "apply - Send a tombstone record (use the record key and value null) (default),"
        + "skip - Record is removed,"
        + "soft - __deleted field is added to the records to flag the deletion.";


    public static final String ADD_FIELDS = "add.fields";
    public static final List<String> ADD_FIELDS_DEFAULT = Collections.emptyList();
    public static final String ADD_FIELDS_DOC = "Adds fields coming from the Qlik Replicate change event headers structure"
        + "Example: 'operation,transactionId' would add __operation, __transactionId fields (Note: the value is converted to string)";

    public static final String ADD_HEADERS = "add.headers";
    public static final List<String> ADD_HEADERS_DEFAULT = Collections.emptyList();
    public static final String ADD_HEADERS_DOC = "Adds fields coming from the Qlik Replicate change event headers structure"
        + "Example: 'operation,transactionId' would add __operation, __transactionId fields (Note: the value is converted to string)";

    public static final ConfigDef.Validator DELETE_MODE_VALIDATOR = (key, value) -> Arrays.stream(DeleteMode.values())
        .filter(mode -> mode.toString().equalsIgnoreCase(value.toString()))
        .findFirst()
        .orElseThrow(() -> new ConfigException(key, value, "Valid values are (" +
            String.join(", ", Arrays.stream(DeleteMode.values()).map(Object::toString).collect(Collectors.toList())) + ")"
        ));

    public static final ConfigDef.Validator CHANGE_EVENT_HEADERS_VALIDATOR = (key, value) -> {
        for (String field : ((List<String>) value)) {
            if (!ChangeEvent.ALL_HEADER_NAMES.contains(field)) {
                throw new ConfigException(key, value, "Valid values are (" + String.join(", ", ChangeEvent.ALL_HEADER_NAMES) + ")");
            }
        }
    };

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            DELETE_MODE,
            ConfigDef.Type.STRING,
            DELETE_MODE_DEFAULT.toString(),
            DELETE_MODE_VALIDATOR,
            ConfigDef.Importance.LOW,
            DELETE_MODE_DOC
        )
        .define(
            ADD_FIELDS,
            ConfigDef.Type.LIST,
            ADD_FIELDS_DEFAULT,
            CHANGE_EVENT_HEADERS_VALIDATOR,
            ConfigDef.Importance.LOW,
            ADD_FIELDS_DOC
        )
        .define(
            ADD_HEADERS,
            ConfigDef.Type.LIST,
            ADD_HEADERS_DEFAULT,
            CHANGE_EVENT_HEADERS_VALIDATOR,
            ConfigDef.Importance.LOW,
            ADD_HEADERS_DOC
        );

    public ExtractNewRecordStateConfig(Map<String, ?> properties) {
        super(CONFIG_DEF, properties);
    }
}
