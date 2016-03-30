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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputWritable;

/**
 * A reducer that writes data into a database using JDBC connection.
 */
public class JdbcExportReducer extends Reducer<LongWritable, JdbcOutputWritable, JdbcOutputWritable, NullWritable> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<JdbcOutputWritable> values, Context context)
            throws IOException, InterruptedException {

        for (JdbcOutputWritable w : values) {
            context.write(w, NullWritable.get());
        }
    }
}
