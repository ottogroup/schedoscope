/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.export.bigquery.outputschema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.bigquery.BigQueryBaseTest;
import org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.configureBigQueryOutputFormat;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat.commit;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputFormat.prepareBigQueryTable;

public class BigQueryOutputFormatTest extends BigQueryBaseTest {

    private HCatSchema flatHcatSchema;

    private Configuration unpartitionedExport, partitionedExport;

    private TaskAttemptContext unpartitionedContext, partitionedContext;

    private DefaultHCatRecord[] inputData;

    private RecordWriter<Object, DefaultHCatRecord> recordWriterPartitioned;
    private RecordWriter<Object, DefaultHCatRecord> recordWriterUnpartitioned;

    @Before
    public void setUp() throws IOException {

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


        unpartitionedExport = configureBigQueryOutputFormat(
                new Configuration(),
                null,
                null,
                "schedoscope_export_big_query_output_test",
                "flat_table",
                null,
                "EU",
                null,
                "aString=y",
                flatHcatSchema,
                "schedoscope_export_big_query_output_test",
                null,
                null,
                1,
                null,
                null
        );

        unpartitionedContext = new TaskAttemptContextImpl(unpartitionedExport, new TaskAttemptID());

        recordWriterUnpartitioned = new BigQueryOutputFormat<Object, DefaultHCatRecord>().getRecordWriter(unpartitionedContext);


        partitionedExport = configureBigQueryOutputFormat(
                new Configuration(),
                null,
                null,
                "schedoscope_export_big_query_output_test",
                "flat_table_part",
                null,
                "EU",
                "20171001",
                "aString=y",
                flatHcatSchema,
                "schedoscope_export_big_query_output_test",
                null,
                null,
                1,
                null,
                null
        );


        partitionedContext = new TaskAttemptContextImpl(partitionedExport, new TaskAttemptID());

        recordWriterPartitioned = new BigQueryOutputFormat<Object, DefaultHCatRecord>().getRecordWriter(partitionedContext);

        inputData = new DefaultHCatRecord[]{
                new DefaultHCatRecord(7) {{
                    set("aString", flatHcatSchema, "someString1");
                    set("anInt", flatHcatSchema, 1);
                    set("aLong", flatHcatSchema, 1L);
                    set("aByte", flatHcatSchema, (byte) 1);
                    set("aBoolean", flatHcatSchema, true);
                    set("aDouble", flatHcatSchema, 1.4d);
                    set("aFloat", flatHcatSchema, 1.5f);

                }},
                new DefaultHCatRecord(7) {{
                    set("aString", flatHcatSchema, "someString2");
                    set("anInt", flatHcatSchema, 2);
                    set("aLong", flatHcatSchema, 2L);
                    set("aByte", flatHcatSchema, (byte) 2);
                    set("aBoolean", flatHcatSchema, true);
                    set("aDouble", flatHcatSchema, 2.4d);
                    set("aFloat", flatHcatSchema, 2.5f);

                }},
                new DefaultHCatRecord(7) {{
                    set("aString", flatHcatSchema, "someString3");
                    set("anInt", flatHcatSchema, 3);
                    set("aLong", flatHcatSchema, 3L);
                    set("aByte", flatHcatSchema, (byte) 3);
                    set("aBoolean", flatHcatSchema, true);
                    set("aDouble", flatHcatSchema, 3.4d);
                    set("aFloat", flatHcatSchema, 3.5f);

                }},
                new DefaultHCatRecord(7) {{
                    set("aString", flatHcatSchema, "someString4");
                    set("anInt", flatHcatSchema, 4);
                    set("aLong", flatHcatSchema, 4L);
                    set("aByte", flatHcatSchema, (byte) 4);
                    set("aBoolean", flatHcatSchema, true);
                    set("aDouble", flatHcatSchema, 4.4d);
                    set("aFloat", flatHcatSchema, 4.5f);

                }},
                new DefaultHCatRecord(7) {{
                    set("aString", flatHcatSchema, "someString5");
                    set("anInt", flatHcatSchema, 5);
                    set("aLong", flatHcatSchema, 5L);
                    set("aByte", flatHcatSchema, (byte) 5);
                    set("aBoolean", flatHcatSchema, true);
                    set("aDouble", flatHcatSchema, 5.4d);
                    set("aFloat", flatHcatSchema, 5.5f);

                }}
        };
    }

    @Test
    public void testUnpartitionedExport() throws IOException, InterruptedException, TimeoutException {
        if (!CALL_BIG_QUERY)
            return;

        prepareBigQueryTable(unpartitionedExport);

        for (DefaultHCatRecord r : inputData)
            recordWriterUnpartitioned.write(null, r);

        recordWriterUnpartitioned.close(unpartitionedContext);

        commit(unpartitionedExport);
    }

    @Test
    public void testPartitionedExport() throws IOException, InterruptedException, TimeoutException {
        if (!CALL_BIG_QUERY)
            return;

        prepareBigQueryTable(partitionedExport);

        for (DefaultHCatRecord r : inputData)
            recordWriterPartitioned.write(null, r);

        recordWriterPartitioned.close(partitionedContext);

        commit(partitionedExport);
    }

}
