/**
 * Copyright 2016 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.schedoscope.export.jdbc;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.jdbc.outputschema.Schema;
import org.schedoscope.export.jdbc.outputschema.SchemaFactory;
import org.schedoscope.export.utils.CustomHCatRecordSerializer;

/**
 * A mapper that reads data from Hive via HCatalog and emits a concatenated
 * string of all HCatRecord fields.
 */
public class JdbcExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, NullWritable> {

    private static final Log LOG = LogFactory.getLog(JdbcExportMapper.class);

    private static final String FIELDSEPARATOR = "\t";

    private HCatSchema inputSchema;

    private String inputFilter;

    private Configuration conf;

    private CustomHCatRecordSerializer serializer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        conf = context.getConfiguration();
        inputSchema = HCatInputFormat.getTableSchema(context.getConfiguration());

        serializer = new CustomHCatRecordSerializer(conf, inputSchema);

        Schema outputSchema = SchemaFactory.getSchema(context.getConfiguration());

        inputFilter = outputSchema.getFilter();

        LOG.info("Used Filter: " + inputFilter);

    }

    @Override
    protected void map(WritableComparable<?> key, HCatRecord value, Context context)
            throws IOException, InterruptedException {

        StringBuilder output = new StringBuilder();

        for (int i = 0; i < value.size(); i++) {
            String fieldValue = "NULL";

            if (value.get(i) != null) {

                if (inputSchema.get(i).isComplex()) {
                    fieldValue = serializer.getJsonComplexType(value, inputSchema.get(i).getName());
                } else {
                    fieldValue = value.get(i).toString();
                }
            }

            output.append(fieldValue);
            output.append(FIELDSEPARATOR);
        }

        if (inputFilter == null) {
            output.append("NULL");
        } else {
            output.append(inputFilter);
        }
        context.write(new Text(output.toString()), NullWritable.get());
    }
}
