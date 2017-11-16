package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.bigquery.BigQueryBaseTest;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class BigQuerySchemaTest extends BigQueryBaseTest {

    private BigQuerySchema bigQuerySchema = new BigQuerySchema();

    private HCatSchema flatHcatSchema, hcatSchemaWithPrimitiveList, hcatSchemaWithStruct, hcatSchemaWithListOfStruct, hcatSchemaWithListOfList, hcatSchemaWithMap, hcatSchemaWithListOfMaps;

    private Schema flatBigQuerySchema, bigQuerySchemaWithPrimitiveList, bigQuerySchemaWithRecord, bigQuerySchemaWithListOfRecord, bigQuerySchemaWithListOfList, bigQuerySchemaWithMap, bigQuerySchemaWithListOfMaps;

    @Before
    public void setUp() throws HCatException {

        PrimitiveTypeInfo hcatStringType = new PrimitiveTypeInfo();
        hcatStringType.setTypeName("string");
        PrimitiveTypeInfo hcatIntType = new PrimitiveTypeInfo();
        hcatIntType.setTypeName("int");
        PrimitiveTypeInfo hcatLongType = new PrimitiveTypeInfo();
        hcatLongType.setTypeName("bigint");
        PrimitiveTypeInfo hcatByteType = new PrimitiveTypeInfo();
        hcatByteType.setTypeName("tinyint");
        PrimitiveTypeInfo hcatBooleanType = new PrimitiveTypeInfo();
        hcatBooleanType.setTypeName("boolean");
        PrimitiveTypeInfo hcatDoubleType = new PrimitiveTypeInfo();
        hcatDoubleType.setTypeName("double");
        PrimitiveTypeInfo hcatFloatType = new PrimitiveTypeInfo();
        hcatFloatType.setTypeName("float");

        flatHcatSchema = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("aString", hcatStringType, "a string field"),
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("aLong", hcatLongType, "a long field"),
                        new HCatFieldSchema("aByte", hcatByteType, "a byte field"),
                        new HCatFieldSchema("aBoolean", hcatBooleanType, "a boolean field"),
                        new HCatFieldSchema("aDouble", hcatDoubleType, "a double field"),
                        new HCatFieldSchema("aFloat", hcatFloatType, "a float field")
                )
        );

        flatBigQuerySchema = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aString", Field.Type.string()).setDescription("a string field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aLong", Field.Type.integer()).setDescription("a long field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aByte", Field.Type.integer()).setDescription("a byte field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aBoolean", Field.Type.bool()).setDescription("a boolean field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aDouble", Field.Type.floatingPoint()).setDescription("a double field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aFloat", Field.Type.floatingPoint()).setDescription("a float field").setMode(Field.Mode.NULLABLE).build()
        );

        hcatSchemaWithPrimitiveList = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("listOfInts",
                                HCatFieldSchema.Type.ARRAY,
                                new HCatSchema(Arrays.asList(new HCatFieldSchema(null, hcatIntType, null))),
                                "a list of ints field")

                )
        );

        bigQuerySchemaWithPrimitiveList = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("listOfInts", Field.Type.integer()).setDescription("a list of ints field").setMode(Field.Mode.REPEATED).build()
        );

        hcatSchemaWithStruct = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("aStruct",
                                HCatFieldSchema.Type.STRUCT,
                                new HCatSchema(
                                        Arrays.asList(
                                                new HCatFieldSchema("aString", hcatStringType, "a string field"),
                                                new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                                                new HCatFieldSchema("aLong", hcatLongType, "a long field"),
                                                new HCatFieldSchema("aByte", hcatByteType, "a byte field"),
                                                new HCatFieldSchema("aBoolean", hcatBooleanType, "a boolean field"),
                                                new HCatFieldSchema("aDouble", hcatDoubleType, "a double field"),
                                                new HCatFieldSchema("aFloat", hcatFloatType, "a float field"),
                                                new HCatFieldSchema(
                                                        "aNestedStruct",
                                                        HCatFieldSchema.Type.STRUCT,
                                                        new HCatSchema(
                                                                Arrays.asList(
                                                                        new HCatFieldSchema("aString", hcatStringType, "a string field")
                                                                )
                                                        ),
                                                        "a nested struct field"
                                                )
                                        )
                                ),
                                "a struct field")

                )
        );

        bigQuerySchemaWithRecord = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aStruct",
                        Field.Type.record(
                                Field.newBuilder("aString", Field.Type.string()).setDescription("a string field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aLong", Field.Type.integer()).setDescription("a long field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aByte", Field.Type.integer()).setDescription("a byte field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aBoolean", Field.Type.bool()).setDescription("a boolean field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aDouble", Field.Type.floatingPoint()).setDescription("a double field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aFloat", Field.Type.floatingPoint()).setDescription("a float field").setMode(Field.Mode.NULLABLE).build(),
                                Field.newBuilder("aNestedStruct",
                                        Field.Type.record(
                                                Field.newBuilder("aString", Field.Type.string()).setDescription("a string field").setMode(Field.Mode.NULLABLE).build()
                                        )
                                ).setDescription("a nested struct field").setMode(Field.Mode.NULLABLE).build()
                        )
                ).setDescription("a struct field").setMode(Field.Mode.NULLABLE).build()
        );


        hcatSchemaWithListOfStruct = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("listOfStructs",
                                HCatFieldSchema.Type.ARRAY,
                                new HCatSchema(
                                        Arrays.asList(
                                                new HCatFieldSchema(
                                                        null,
                                                        HCatFieldSchema.Type.STRUCT,
                                                        new HCatSchema(
                                                                Arrays.asList(
                                                                        new HCatFieldSchema("aString", hcatStringType, "a string field")
                                                                )
                                                        ),
                                                        null
                                                )
                                        )
                                ),
                                "a list of structs field")
                )
        );

        bigQuerySchemaWithListOfRecord = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("listOfStructs",
                        Field.Type.record(
                                Field.newBuilder("aString", Field.Type.string()).setDescription("a string field").setMode(Field.Mode.NULLABLE).build()
                        )
                ).setDescription("a list of structs field").setMode(Field.Mode.REPEATED).build()
        );

        hcatSchemaWithListOfList = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("listOfList",
                                HCatFieldSchema.Type.ARRAY,
                                new HCatSchema(
                                        Arrays.asList(
                                                new HCatFieldSchema(null,
                                                        HCatFieldSchema.Type.ARRAY,
                                                        new HCatSchema(Arrays.asList(new HCatFieldSchema(null, hcatIntType, null))),
                                                        null)
                                        )
                                ),
                                "a list of lists field")

                )
        );

        bigQuerySchemaWithListOfList = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("listOfList", Field.Type.string()).setDescription("a list of lists field").setMode(Field.Mode.REPEATED).build()
        );

        hcatSchemaWithMap = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("aMap",
                                HCatFieldSchema.Type.MAP,
                                HCatFieldSchema.Type.STRING,
                                new HCatSchema(
                                        Arrays.asList(
                                                new HCatFieldSchema(null, hcatStringType, null)
                                        )
                                ),
                                "a map field")

                )
        );

        bigQuerySchemaWithMap = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("aMap", Field.Type.string()).setDescription("a map field").setMode(Field.Mode.NULLABLE).build()
        );

        hcatSchemaWithListOfMaps = new HCatSchema(
                Arrays.asList(
                        new HCatFieldSchema("anInt", hcatIntType, "an int field"),
                        new HCatFieldSchema("listOfMap",
                                HCatFieldSchema.Type.ARRAY,
                                new HCatSchema(
                                        Arrays.asList(
                                                new HCatFieldSchema(null,
                                                        HCatFieldSchema.Type.MAP,
                                                        HCatFieldSchema.Type.STRING,
                                                        new HCatSchema(
                                                                Arrays.asList(
                                                                        new HCatFieldSchema(null, hcatStringType, null)
                                                                )
                                                        ),
                                                        null)
                                        )
                                ),
                                "a list of maps field")

                )
        );

        bigQuerySchemaWithListOfMaps = Schema.of(
                Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setDescription("HCatInputFormat filter used to export the present record.").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("listOfMap", Field.Type.string()).setDescription("a list of maps field").setMode(Field.Mode.REPEATED).build()
        );
    }

    @Test
    public void testFlatTableConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "flat_table", flatHcatSchema);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("flat_table", converted.getTableId().getTable());
        assertEquals(flatBigQuerySchema, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithPrimitiveListConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_primitive_list", hcatSchemaWithPrimitiveList);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_primitive_list", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithPrimitiveList, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithStructConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_struct", hcatSchemaWithStruct);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_struct", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithRecord, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListStructConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_struct", hcatSchemaWithListOfStruct);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_struct", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfRecord, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListOfListsConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_of_lists", hcatSchemaWithListOfList);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_of_lists", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfList, converted.getDefinition().getSchema());

        createTable(converted);
    }


    @Test
    public void testTableWithMapConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_map", hcatSchemaWithMap);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_map", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithMap, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListOfMapConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_of_map", hcatSchemaWithListOfMaps);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_of_map", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfMaps, converted.getDefinition().getSchema());

        createTable(converted);
    }
}
