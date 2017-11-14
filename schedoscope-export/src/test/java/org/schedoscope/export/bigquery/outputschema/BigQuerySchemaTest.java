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

public class BigQuerySchemaTest {

    private BigQuerySchema bigQuerySchema = new BigQuerySchema();

    private BigQuery bigQuery;

    private HCatSchema flatHcatSchema;

    private Schema flatBigQuerySchema;

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

        HCatFieldSchema hcatStringField = new HCatFieldSchema("aString", hcatStringType, "a string field");
        HCatFieldSchema hcatIntField = new HCatFieldSchema("anInt", hcatIntType, "an int field");
        HCatFieldSchema hcatLongField = new HCatFieldSchema("aLong", hcatLongType, "a long field");
        HCatFieldSchema hcatByteField = new HCatFieldSchema("aByte", hcatByteType, "a byte field");
        HCatFieldSchema hcatBooleanField = new HCatFieldSchema("aBoolean", hcatBooleanType, "a boolean field");
        HCatFieldSchema hcatDoubleField = new HCatFieldSchema("aDouble", hcatDoubleType, "a double field");
        HCatFieldSchema hcatFloatField = new HCatFieldSchema("aFloat", hcatFloatType, "a float field");

        flatHcatSchema = new HCatSchema(
                Arrays.asList(
                        hcatStringField,
                        hcatIntField,
                        hcatLongField,
                        hcatByteField,
                        hcatBooleanField,
                        hcatDoubleField,
                        hcatFloatField
                )
        );

        flatBigQuerySchema = Schema.of(
                Field.newBuilder("aString", Field.Type.string()).setDescription("a string field").build(),
                Field.newBuilder("anInt", Field.Type.integer()).setDescription("an int field").build(),
                Field.newBuilder("aLong", Field.Type.integer()).setDescription("a long field").build(),
                Field.newBuilder("aByte", Field.Type.integer()).setDescription("a byte field").build(),
                Field.newBuilder("aBoolean", Field.Type.bool()).setDescription("a boolean field").build(),
                Field.newBuilder("aDouble", Field.Type.floatingPoint()).setDescription("a double field").build(),
                Field.newBuilder("aFloat", Field.Type.floatingPoint()).setDescription("a float field").build()
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
        bigQuery.create(
            bigQuerySchema.convertSchemaToTableInfo("schedoscope_export_big_query_schema_test", "flat_table", flatHcatSchema)
        );
    }
}
