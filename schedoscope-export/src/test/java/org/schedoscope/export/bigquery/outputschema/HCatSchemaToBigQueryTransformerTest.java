package org.schedoscope.export.bigquery.outputschema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.bigquery.BigQueryBaseTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.convertSchemaToTableInfo;


public class HCatSchemaToBigQueryTransformerTest extends BigQueryBaseTest {

    private HCatSchema flatHcatSchema, hcatSchemaWithPrimitiveList, hcatSchemaWithStruct, hcatSchemaWithListOfStruct, hcatSchemaWithListOfList, hcatSchemaWithMap, hcatSchemaWithListOfMaps;

    private Schema flatBigQuerySchema, bigQuerySchemaWithPrimitiveList, bigQuerySchemaWithRecord, bigQuerySchemaWithListOfRecord, bigQuerySchemaWithListOfList, bigQuerySchemaWithMap, bigQuerySchemaWithListOfMaps;

    private DefaultHCatRecord flatHcatRecord, hcatRecordWithPrimitiveList, hCatRecordWithStruct, hcatRecordWithListOfStruct, hcatRecordWithListOfList, hcatRecordWithMap, hcatRecordWithListOfMap;

    private Map<String, Object> flatBigQueryRecord, bigQueryRecordWithPrimitiveList, bigQueryRecordWithStruct, bigQueryRecordWithListOfStruct, bigQueryRecordWithListOfList, bigQueryRecordWithMap, bigQueryRecordWithListOfMap;

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


        flatHcatRecord = new DefaultHCatRecord(7) {{
            set("aString", flatHcatSchema, "someString");
            set("anInt", flatHcatSchema, 1);
            set("aLong", flatHcatSchema, 2L);
            set("aByte", flatHcatSchema, (byte) 3);
            set("aBoolean", flatHcatSchema, true);
            set("aDouble", flatHcatSchema, 3.4d);
            set("aFloat", flatHcatSchema, 3.5f);

        }};

        flatBigQueryRecord = new HashMap<String, Object>() {{
            put("aString", "someString");
            put("anInt", 1);
            put("aLong", 2L);
            put("aByte", (byte) 3);
            put("aBoolean", true);
            put("aDouble", 3.4d);
            put("aFloat", 3.5f);
        }};

        hcatRecordWithPrimitiveList = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithPrimitiveList, 2);
            set("listOfInts", hcatSchemaWithPrimitiveList, Arrays.asList(1, 2, 3));
        }};

        bigQueryRecordWithPrimitiveList = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("listOfInts", Arrays.asList(1, 2, 3));
        }};

        hCatRecordWithStruct = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithStruct, 2);
            set("aStruct", hcatSchemaWithStruct, Arrays.asList(
                    "someString",
                    1,
                    2L,
                    (byte) 3,
                    true,
                    3.14d,
                    3.14f,
                    Arrays.asList(
                            "someMoreString"
                    )
            ));
        }};

        bigQueryRecordWithStruct = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("aStruct", new HashMap<String, Object>() {{
                        put("aString", "someString");
                        put("anInt", 1);
                        put("aLong", 2L);
                        put("aByte", (byte) 3);
                        put("aBoolean", true);
                        put("aDouble", 3.14d);
                        put("aFloat", 3.14f);
                        put("aNestedStruct", new HashMap<String, Object>() {{
                            put("aString", "someMoreString");
                        }});
                    }}
            );
        }};

        hcatRecordWithListOfStruct = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithListOfStruct, 2);
            set("listOfStructs", hcatSchemaWithListOfStruct, Arrays.asList(
                    Arrays.asList("someString"),
                    Arrays.asList("someMoreString"),
                    Arrays.asList("someMoreAndMoreString"),
                    Arrays.asList("evenSomeMoreString")
            ));
        }};

        bigQueryRecordWithListOfStruct = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("listOfStructs", Arrays.asList(
                    new HashMap<String, Object>() {{
                        put("aString", "someString");
                    }},
                    new HashMap<String, Object>() {{
                        put("aString", "someMoreString");
                    }},
                    new HashMap<String, Object>() {{
                        put("aString", "someMoreAndMoreString");
                    }},
                    new HashMap<String, Object>() {{
                        put("aString", "evenSomeMoreString");
                    }}
            ));
        }};

        hcatRecordWithListOfList = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithListOfList, 2);
            set("listOfList", hcatSchemaWithListOfList, Arrays.asList(
                    Arrays.asList(1, 2, 3, 4),
                    Arrays.asList(5, 6, 7, 8),
                    Arrays.asList(9, 10, 11, 12)
            ));
        }};

        bigQueryRecordWithListOfList = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("listOfList", Arrays.asList(
                    "[1,2,3,4]",
                    "[5,6,7,8]",
                    "[9,10,11,12]"
            ));
        }};

        hcatRecordWithMap = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithMap, 2);
            set("aMap", hcatSchemaWithMap, new HashMap<String, Integer>() {{
                put("a", 1);
                put("b", 2);
                put("c", 3);
            }});
        }};

        bigQueryRecordWithMap = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("aMap", "{" +
                    "\"a\":1," +
                    "\"b\":2," +
                    "\"c\":3" +
                    "}");
        }};

        hcatRecordWithListOfMap = new DefaultHCatRecord(2) {{
            set("anInt", hcatSchemaWithListOfMaps, 2);
            set("listOfMap", hcatSchemaWithListOfMaps, Arrays.asList(
                    new HashMap<String, Integer>() {{
                        put("a", 1);
                        put("b", 2);
                        put("c", 3);
                    }},
                    new HashMap<String, Integer>() {{
                        put("d", 4);
                        put("e", 5);
                        put("f", 6);
                    }},
                    new HashMap<String, Integer>() {{
                        put("g", 7);
                        put("h", 8);
                        put("i", 9);
                    }})
            );
        }};

        bigQueryRecordWithListOfMap = new HashMap<String, Object>() {{
            put("anInt", 2);
            put("listOfMap", Arrays.asList(
                    "{" +
                            "\"a\":1," +
                            "\"b\":2," +
                            "\"c\":3" +
                            "}",
                    "{" +
                            "\"d\":4," +
                            "\"e\":5," +
                            "\"f\":6" +
                            "}",
                    "{" +
                            "\"g\":7," +
                            "\"h\":8," +
                            "\"i\":9" +
                            "}"

                    )
            );
        }};


    }

    @Test
    public void testFlatTableConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "flat_table", flatHcatSchema);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("flat_table", converted.getTableId().getTable());
        assertEquals(flatBigQuerySchema, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableConversionWithPartitioning() throws IOException, NoSuchFieldException, IllegalAccessException {
        PartitioningScheme partitioning = PartitioningScheme.MONTHLY;

        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "flat_table", flatHcatSchema, partitioning);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("flat_table", converted.getTableId().getTable());

        StandardTableDefinition bigQueryTableDefinition = converted.getDefinition();

        java.lang.reflect.Field field = StandardTableDefinition.class.getDeclaredField("timePartitioning");
        field.setAccessible(true);
        TimePartitioning timePartitioning = (TimePartitioning) field.get(bigQueryTableDefinition);

        assertEquals(TimePartitioning.Type.DAY, timePartitioning.getType());

    }


    @Test
    public void testTableWithPrimitiveListConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_primitive_list", hcatSchemaWithPrimitiveList);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_primitive_list", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithPrimitiveList, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithStructConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_struct", hcatSchemaWithStruct);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_struct", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithRecord, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListStructConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_struct", hcatSchemaWithListOfStruct);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_struct", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfRecord, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListOfListsConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_of_lists", hcatSchemaWithListOfList);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_of_lists", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfList, converted.getDefinition().getSchema());

        createTable(converted);
    }


    @Test
    public void testTableWithMapConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_map", hcatSchemaWithMap);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_map", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithMap, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testTableWithListOfMapConversion() throws IOException {
        TableInfo converted = convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_list_of_map", hcatSchemaWithListOfMaps);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_list_of_map", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithListOfMaps, converted.getDefinition().getSchema());

        createTable(converted);
    }

    @Test
    public void testFlatHCatRecordConversion() throws IOException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(flatHcatSchema, flatHcatRecord);

        assertEquals(flatBigQueryRecord, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "flat_table", flatBigQuerySchema, converted);
    }

    @Test
    public void testHCatRecordWithListConversion() throws IOException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithPrimitiveList, hcatRecordWithPrimitiveList);

        assertEquals(bigQueryRecordWithPrimitiveList, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_primitive_list", bigQuerySchemaWithPrimitiveList, converted);
    }

    @Test
    public void testHCatRecordWithStructConversion() throws JsonProcessingException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithStruct, hCatRecordWithStruct);

        assertEquals(bigQueryRecordWithStruct, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_struct", bigQuerySchemaWithRecord, converted);
    }

    @Test
    public void testHCatRecordWithListOfStructConversion() throws JsonProcessingException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithListOfStruct, hcatRecordWithListOfStruct);

        assertEquals(bigQueryRecordWithListOfStruct, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_list_struct", bigQuerySchemaWithListOfRecord, converted);
    }

    @Test
    public void testHCatRecordWithListOfListConversion() throws JsonProcessingException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithListOfList, hcatRecordWithListOfList);

        assertEquals(bigQueryRecordWithListOfList, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_list_of_lists", bigQuerySchemaWithListOfList, converted);
    }

    @Test
    public void testHCatRecordWithMapConversion() throws JsonProcessingException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithMap, hcatRecordWithMap);

        assertEquals(bigQueryRecordWithMap, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_map", bigQuerySchemaWithMap, converted);
    }

    @Test
    public void testHCatRecordWithListOfMapConversion() throws JsonProcessingException {
        Map<String, Object> converted = convertHCatRecordToBigQueryMap(hcatSchemaWithListOfMaps, hcatRecordWithListOfMap);

        assertEquals(bigQueryRecordWithListOfMap, converted);

        insertIntoTable("schedoscope_export_big_query_record_test", "table_with_list_of_map", bigQuerySchemaWithListOfMaps, converted);

        System.out.println("cdsddsds");
    }
}
