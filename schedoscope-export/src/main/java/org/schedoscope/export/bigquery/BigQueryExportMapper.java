package org.schedoscope.export.bigquery;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.util.Map;

import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.getBigQueryHCatSchema;
import static org.schedoscope.export.bigquery.outputformat.BigQueryOutputConfiguration.getBigQueryUsedHcatFilter;
import static org.schedoscope.export.bigquery.outputschema.HCatRecordToBigQueryMapConvertor.convertHCatRecordToBigQueryMap;
import static org.schedoscope.export.bigquery.outputschema.HCatSchemaToBigQuerySchemaConverter.USED_FILTER_FIELD_NAME;

/**
 * Mapper that transforms an HCatRecord to an equivalent BigQuery JSON-formatted record.
 */
public class BigQueryExportMapper extends Mapper<WritableComparable<?>, HCatRecord, LongWritable, Text> {

    private HCatSchema hcatSchema;
    private String usedHCatFilter;
    private ObjectMapper jsonFactory = new ObjectMapper();


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        hcatSchema = getBigQueryHCatSchema(conf);
        usedHCatFilter = getBigQueryUsedHcatFilter(conf);

    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value,
                       Context context) throws IOException, InterruptedException {

        Map<String, Object> recordMap = convertHCatRecordToBigQueryMap(hcatSchema, value);
        recordMap.put(USED_FILTER_FIELD_NAME, this.usedHCatFilter);

        Text outputValue = new Text(jsonFactory.writeValueAsString(recordMap) + "\n");

        LongWritable outputKey = new LongWritable(outputValue.hashCode());

        context.write(outputKey, outputValue);
    }
}
