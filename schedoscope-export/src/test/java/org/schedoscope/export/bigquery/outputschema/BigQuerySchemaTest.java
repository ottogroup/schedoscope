package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class BigQuerySchemaTest {

    private BigQuerySchema bigQuerySchema = new BigQuerySchema();

    private BigQuery bigQuery;

    private HCatSchema flatHcatSchema, hcatSchemaWithPrimitiveList;

    private Schema flatBigQuerySchema, bigQuerySchemaWithPrimitiveList;

    @Before
    public void setUp() throws HCatException {

        bigQuery = BigQueryOptions.getDefaultInstance().getService();

        createBigQueryDataSet();

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
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").setMode(Field.Mode.NULLABLE).build(),
                Field.newBuilder("listOfInts", Field.Type.integer()).setDescription("a list of ints field").setMode(Field.Mode.REPEATED).build()
        );

    }

    public void createBigQueryDataSet() {
        dropBigQueryDataSets();
        DatasetInfo datasetInfo = DatasetInfo.newBuilder("schedoscope_export_big_query_schema_test").build();
        bigQuery.create(datasetInfo);
    }

    //@After
    public void dropBigQueryDataSets() {
        DatasetId datasetId = DatasetId.of("schedoscope_export_big_query_schema_test");
        bigQuery.delete(datasetId, BigQuery.DatasetDeleteOption.deleteContents());
    }

    @Test
    public void testFlatTableConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "flat_table", flatHcatSchema);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("flat_table", converted.getTableId().getTable());
        assertEquals(flatBigQuerySchema, converted.getDefinition().getSchema());
    }

    @Test
    public void testTableWithPrimitiveListConversion() throws IOException {
        TableInfo converted = bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "table_with_primitive_list", hcatSchemaWithPrimitiveList);

        assertEquals("schedoscope_export_big_query_schema_test", converted.getTableId().getDataset());
        assertEquals("table_with_primitive_list", converted.getTableId().getTable());
        assertEquals(bigQuerySchemaWithPrimitiveList, converted.getDefinition().getSchema());

        bigQuery.create(converted);
    }
}
