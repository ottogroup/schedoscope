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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.outputformat.JdbcOutputFormat;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.outputschema.SchemaUtils;

public class JdbcExportJobMRFullTest extends HiveUnitBaseTest {

    private static final String JDBC_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";

    private static final String CREATE_CONNECTION_STRING = "jdbc:derby:memory:TestingDB;create=true";

    private static final String CONNECTION_STRING = "jdbc:derby:memory:TestingDB";

    private static final int NUM_PARTITIONS = 2;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        setUpHiveServer("src/test/resources/ogm_event_features_data.txt", "src/test/resources/ogm_event_features.hql",
                "ogm_event_features");

        Class.forName(JDBC_DRIVER_NAME);
        DriverManager.getConnection(CREATE_CONNECTION_STRING);
    }

    @Test
    public void testRunMrJob() throws Exception {

        Job job = Job.getInstance(conf);

        job.setMapperClass(JdbcExportMapper.class);
        job.setReducerClass(JdbcExportReducer.class);
        job.setNumReduceTasks(NUM_PARTITIONS);

        Schema outputSchema = SchemaFactory.getSchema(CONNECTION_STRING, job.getConfiguration());

        String[] columnNames = SchemaUtils.getColumnNamesFromHcatSchema(hcatInputSchema, outputSchema);
        String[] columnTypes = SchemaUtils.getColumnTypesFromHcatSchema(hcatInputSchema, outputSchema);

        JdbcOutputFormat.setOutput(job.getConfiguration(), CONNECTION_STRING, null, null, "testing", null,
                NUM_PARTITIONS, 10000, null, null, columnNames, columnTypes);

        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(JdbcOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(JdbcOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        assertTrue(job.waitForCompletion(true));
        JdbcOutputFormat.finalizeOutput(job.getConfiguration());

        Connection conn = outputSchema.getConnection();
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM testing");
        while (rs.next()) {
            assertEquals(10, rs.getInt(1));
        }
    }
}
