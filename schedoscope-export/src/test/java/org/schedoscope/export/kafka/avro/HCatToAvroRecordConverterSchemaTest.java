/**
 * Copyright 2016 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.kafka.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HCatToAvroRecordConverterSchemaTest {

    PrimitiveTypeInfo stringType = new PrimitiveTypeInfo();
    PrimitiveTypeInfo longType = new PrimitiveTypeInfo();
    PrimitiveTypeInfo intType = new PrimitiveTypeInfo();

    HCatSchema subSchemaMulti;
    HCatSchema subSchemaSingle;

    @Before
    public void setUp() throws Exception {

        stringType.setTypeName("string");
        longType.setTypeName("bigint");
        intType.setTypeName("int");

        // create nested sub schema
        List<HCatFieldSchema> fields = new ArrayList<HCatFieldSchema>();
        HCatFieldSchema field1 = new HCatFieldSchema("nested_field1", longType,
                "comment");
        HCatFieldSchema field2 = new HCatFieldSchema("nested_field2",
                stringType, "comment");
        HCatFieldSchema field3 = new HCatFieldSchema("nested_field3", intType,
                "comment");
        fields.add(field1);
        subSchemaSingle = new HCatSchema(fields);
        fields.add(field2);
        fields.add(field3);
        subSchemaMulti = new HCatSchema(fields);
    }

    @Test
    public void testStructSchemaConversion() throws IOException {

        List<HCatFieldSchema> fields = new ArrayList<HCatFieldSchema>();
        HCatFieldSchema field1 = new HCatFieldSchema("field1", longType,
                "comment");
        HCatFieldSchema field2 = new HCatFieldSchema("field2", stringType,
                "comment");
        HCatFieldSchema field3 = new HCatFieldSchema("field3", intType,
                "comment");
        HCatFieldSchema field4 = new HCatFieldSchema("field4",
                HCatFieldSchema.Type.STRUCT, subSchemaMulti, "comment");
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        fields.add(field4);

        HCatSchema schemaComplete = new HCatSchema(fields);
        HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter();
        Schema avroSchema = schemaConverter.convertSchema(schemaComplete,
                "my_table");

        assertEquals("my_table", avroSchema.getName());

        assertEquals(4, avroSchema.getFields().size());
        assertEquals(schemaComplete.getSchemaAsTypeString(),
                avroSchema.getDoc());

        assertEquals(Schema.create(Schema.Type.INT),
                avroSchema.getField("field4").schema().getTypes().get(1)
                        .getField("nested_field3").schema().getTypes().get(1));
        assertEquals(Schema.create(Schema.Type.STRING),
                avroSchema.getField("field4").schema().getTypes().get(1)
                        .getField("nested_field2").schema().getTypes().get(1));
        assertEquals(Schema.create(Schema.Type.LONG),
                avroSchema.getField("field1").schema().getTypes().get(1));
    }

    @Test
    public void testArraySchemaConversion() throws IOException {

        List<HCatFieldSchema> fields = new ArrayList<HCatFieldSchema>();
        HCatFieldSchema field1 = new HCatFieldSchema("field1", longType,
                "comment");
        HCatFieldSchema field2 = new HCatFieldSchema("field2", stringType,
                "comment");
        HCatFieldSchema field3 = new HCatFieldSchema("field3", intType,
                "comment");
        HCatFieldSchema field4 = new HCatFieldSchema("field4",
                HCatFieldSchema.Type.ARRAY, subSchemaSingle, "comment");
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        fields.add(field4);

        HCatSchema schemaComplete = new HCatSchema(fields);
        HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter();
        Schema avroSchema = schemaConverter.convertSchema(schemaComplete,
                "my_table");

        assertEquals("my_table", avroSchema.getName());
        assertEquals(4, avroSchema.getFields().size());
        assertEquals(schemaComplete.getSchemaAsTypeString(),
                avroSchema.getDoc());

        assertEquals(Schema.Type.ARRAY, avroSchema.getField("field4").schema()
                .getTypes().get(1).getType());
        assertEquals(Schema.create(Schema.Type.LONG),
                avroSchema.getField("field4").schema().getTypes().get(1)
                        .getElementType().getTypes().get(1));
        assertEquals(Schema.create(Schema.Type.LONG),
                avroSchema.getField("field1").schema().getTypes().get(1));
    }

    @Test
    public void testMapSchemaConversion() throws IOException {

        List<HCatFieldSchema> fields = new ArrayList<HCatFieldSchema>();
        HCatFieldSchema field1 = new HCatFieldSchema("field1", longType,
                "comment");
        HCatFieldSchema field2 = new HCatFieldSchema("field2", stringType,
                "comment");
        HCatFieldSchema field3 = new HCatFieldSchema("field3", intType,
                "comment");
        HCatFieldSchema field4 = HCatFieldSchema.createMapTypeFieldSchema(
                "field4", stringType, subSchemaSingle, "comment");
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        fields.add(field4);

        HCatSchema schemaComplete = new HCatSchema(fields);
        HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter();
        Schema avroSchema = schemaConverter.convertSchema(schemaComplete,
                "my_table");

        assertEquals("my_table", avroSchema.getName());
        assertEquals(4, avroSchema.getFields().size());
        assertEquals(schemaComplete.getSchemaAsTypeString(),
                avroSchema.getDoc());

        assertEquals(Schema.Type.MAP, avroSchema.getField("field4").schema()
                .getTypes().get(1).getType());
        assertEquals(Schema.create(Schema.Type.LONG),
                avroSchema.getField("field4").schema().getTypes().get(1)
                        .getValueType().getTypes().get(1));
        assertEquals(Schema.create(Schema.Type.LONG),
                avroSchema.getField("field1").schema().getTypes().get(1));
    }

    @Test
    public void testStuctStructSchemaConversion() throws IOException {

        List<HCatFieldSchema> fields = new ArrayList<HCatFieldSchema>();
        HCatFieldSchema field1 = new HCatFieldSchema("field1", longType,
                "comment");
        HCatFieldSchema field2 = new HCatFieldSchema("field2", stringType,
                "comment");
        HCatFieldSchema field3 = new HCatFieldSchema("field3", intType,
                "comment");
        HCatFieldSchema field4 = new HCatFieldSchema("field4",
                HCatFieldSchema.Type.STRUCT, subSchemaMulti, "comment");
        fields.add(field1);
        fields.add(field2);
        fields.add(field3);
        fields.add(field4);

        HCatSchema subStructSchema = new HCatSchema(fields);

        HCatFieldSchema field5 = new HCatFieldSchema("field5",
                HCatFieldSchema.Type.STRUCT, subStructSchema, "comment");
        fields.add(field5);

        HCatSchema schemaComplete = new HCatSchema(fields);
        HCatToAvroSchemaConverter schemaConverter = new HCatToAvroSchemaConverter();
        Schema avroSchema = schemaConverter.convertSchema(schemaComplete,
                "my_table");

        assertEquals(5, avroSchema.getFields().size());
        assertEquals(schemaComplete.getSchemaAsTypeString(),
                avroSchema.getDoc());
    }
}
