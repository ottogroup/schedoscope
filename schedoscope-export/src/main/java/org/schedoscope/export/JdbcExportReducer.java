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

package org.schedoscope.export;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;

/**
 * A reducer that writes data into a database using JDBC connection.
 */
public class JdbcExportReducer extends Reducer<Text, NullWritable, JdbcOutputWritable, NullWritable> {

    private String[] columnTypes;
    private Map<String, String> preparedStatementTypeMapping;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        Schema outputSchema = SchemaFactory.getSchema(context.getConfiguration());
        columnTypes = outputSchema.getColumnTypes();
        preparedStatementTypeMapping = outputSchema.getPreparedStatementTypeMapping();

    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        String[] line = key.toString().split("\t");

        if (line.length == columnTypes.length) {
            JdbcOutputWritable output = new JdbcOutputWritable(line, columnTypes, preparedStatementTypeMapping);
            context.write(output, NullWritable.get());
        }
    }
}
