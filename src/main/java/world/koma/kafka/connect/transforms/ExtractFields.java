package world.koma.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

public abstract class ExtractFields<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Extract the specified fields from a Struct when schema present, or a Map in the case of schemaless data. "
                    + "The extracted fields can be renamed."
                    + "<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class.getName() + "</code>) "
                    + "or value (<code>" + Value.class.getName() + "</code>).";

    private static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                    new ConfigDef.Validator() {
                        @SuppressWarnings("unchecked")
                        @Override
                        public void ensureValid(String name, Object value) {
                            parseFieldNames((List<String>) value);
                        }

                        @Override
                        public String toString() {
                            return "list of strings or colon-delimited pairs, e.g. <code>abc,def:xyz</code>";
                        }
                    },
                    ConfigDef.Importance.MEDIUM, "Field names to extract, optionally with rename mappings");

    private static final String PURPOSE = "field extraction and renaming";

    private Map<String, List<String>> fieldNames;
    private Map<String, Object> fieldNamesReversed;
    private Cache<Schema, Schema> schemaUpdateCache;



    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldNames = parseFieldNames(config.getList(FIELDS_CONFIG));
        fieldNamesReversed = invert(fieldNames);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    static Map<String, List<String>> parseFieldNames(List<String> names) {
        final var f = new HashMap<String, List<String>>();
        for (String name : names) {
            final String[] parts = name.split(":");
            switch (parts.length) {
                case 1: f.put(parts[0], List.of(name)); break;
                case 2: f.put(parts[0], Arrays.asList(parts[1].split("\\."))); break;
                default: throw new ConfigException(FIELDS_CONFIG, name, "Invalid field name: " + name);
            }
        }
        return f;
    }

    static Map<String, Object> invert(Map<String, List<String>> fieldNames) {
        final Map<String, Object> rootMap = new HashMap<>();

        for (var e : fieldNames.entrySet()) {

            final var sourceName = e.getKey();
            final var targetPath = e.getValue();

            var currentMap = rootMap;

            for (var i = targetPath.iterator(); i.hasNext();) {
                var namePart = i.next();
                if (i.hasNext()) {
                    var nestedMap = currentMap.get(namePart);
                    if (!(nestedMap instanceof Map)) {
                        nestedMap = new HashMap<String, Object>();
                        currentMap.put(namePart, nestedMap);
                    }
                    currentMap = (Map<String, Object>) nestedMap;
                } else {
                    currentMap.put(namePart, sourceName);
                }
            }

        }

        return rootMap;
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        } else if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(fieldNames.size());

        for (Map.Entry<String, Object> e : value.entrySet()) {
            final String fieldName = e.getKey();
            if (fieldNames.containsKey(fieldName)) {

                final var fieldValue = e.getValue();
                final var targetPath = fieldNames.get(fieldName);
                var valueMap = updatedValue;

                for (var i = targetPath.iterator(); i.hasNext();) {
                    var namePart = i.next();
                    var isList = false;
                    if (namePart.endsWith("[0]")) {
                        isList = true;
                        namePart = namePart.substring(0, namePart.length() - 3);
                    }
                    if (i.hasNext()) {
                        var nestedMap = valueMap.get(namePart);
                        if (!valueMap.containsKey(namePart)) {
                            nestedMap = new HashMap<String, Object>();
                            if (isList) {
                                valueMap.put(namePart, List.of(nestedMap));
                            } else {
                                valueMap.put(namePart, nestedMap);
                            }
                        }
                        valueMap = (Map<String, Object>) nestedMap;
                    } else {
                        valueMap.put(namePart, fieldValue);
                    }
                }
            }
        }

        return newRecord(record, null, updatedValue);
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStructOrNull(operatingValue(record), PURPOSE);

        Schema targetSchema = schemaUpdateCache.get(value.schema());
        if (targetSchema == null) {
            targetSchema = makeTargetSchema(value.schema());
            schemaUpdateCache.put(value.schema(), targetSchema);
        }

        final var targetValue = buildWithSchema(value, targetSchema, fieldNamesReversed);
        return newRecord(record, targetSchema, targetValue);

    }

    private Schema makeTargetSchema(Schema sourceSchema) {
        return SchemaUtil.copySchemaBasics(sourceSchema, makeTargetSchema(sourceSchema, fieldNamesReversed));
    }

    private SchemaBuilder makeTargetSchema(Schema sourceSchema, Map<String, Object> targetPaths) {
        final var struct = SchemaBuilder.struct();
        for (var field: targetPaths.entrySet()) {
            var fieldName = field.getKey();
            var isArray = false;
            if (fieldName.endsWith("[0]")) {
                isArray = true;
                fieldName = fieldName.substring(0, fieldName.length() - 3);
            }
            if (field.getValue() instanceof Map) {
                if (isArray) {
                    struct.field(fieldName, SchemaBuilder.array(makeTargetSchema(sourceSchema, (Map<String, Object>) field.getValue())));
                } else {
                    struct.field(fieldName, makeTargetSchema(sourceSchema, (Map<String, Object>) field.getValue()));
                }
            } else {
                struct.field(fieldName, sourceSchema.field((String) field.getValue()).schema());
            }
        }
        return struct;
    }

    private Object buildWithSchema(Struct record, Schema targetSchema, Map<String, Object> targetPaths) {
        if (targetSchema.type() == Schema.Type.ARRAY) {
            return List.of(buildWithSchema(record, targetSchema.valueSchema(), targetPaths));
        } else {
            final Struct struct = new Struct(targetSchema);
            for (var field : targetPaths.entrySet()) {
                var fieldName = field.getKey();
                if (fieldName.endsWith("[0]")) {
                    fieldName = fieldName.substring(0, fieldName.length() - 3);
                }
                if (field.getValue() instanceof Map) {
                    struct.put(fieldName,
                            buildWithSchema(record, targetSchema.field(fieldName).schema(), (Map<String, Object>) field.getValue()));
                } else {
                    struct.put(fieldName, record.get((String) field.getValue()));
                }
            }
            return struct;
        }
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtractFields<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractFields<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
