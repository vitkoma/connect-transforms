package world.koma.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExtractFieldsTest {

    private final ExtractFields<SinkRecord> xform = new ExtractFields.Value<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "abc:xyz,foo:bar");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void schemaless() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "abc:xyz,foo:nested.bar,fox:nested.box,etc:another.nested.etc");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("fox", false);
        value.put("etc", "bury");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedValue = (Map<String, Object>) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertEquals(42, updatedValue.get("xyz"));
        assertEquals(true, ((Map) updatedValue.get("nested")).get("bar"));
        assertEquals(false, ((Map) updatedValue.get("nested")).get("box"));
        assertEquals("bury", ((Map) ((Map) updatedValue.get("another")).get("nested")).get("etc"));
    }

    @Test
    public void withSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("fields", "abc:xyz,foo:nested.bar,fox:nested.box,etc:another.nested.etc");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("fox", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("fox", false);
        value.put("etc", "bury");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(3, updatedValue.schema().fields().size());
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("xyz"));
        assertEquals(true, updatedValue.getStruct("nested").getBoolean("bar"));
        assertEquals(false, updatedValue.getStruct("nested").getBoolean("box"));
        assertEquals("bury", updatedValue.getStruct("another").getStruct("nested").getString("etc"));
    }
}