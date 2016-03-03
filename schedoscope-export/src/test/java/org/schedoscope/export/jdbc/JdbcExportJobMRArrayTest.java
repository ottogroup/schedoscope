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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.jdbc.outputformat.JdbcOutputWritable;

public class JdbcExportJobMRArrayTest extends HiveUnitBaseTest {

    MapDriver<WritableComparable<?>, HCatRecord, Text, NullWritable> mapDriver;
    ReduceDriver<Text, NullWritable, JdbcOutputWritable, NullWritable> reduceDriver;
    MapReduceDriver<WritableComparable<?>, HCatRecord, Text, NullWritable, JdbcOutputWritable, NullWritable> mapReduceDriver;

    @Override
    @SuppressWarnings("deprecation")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        JdbcExportMapper mapper = new JdbcExportMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.setConfiguration(conf);

        JdbcExportReducer reducer = new JdbcExportReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        reduceDriver.setConfiguration(conf);

        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        mapReduceDriver.setConfiguration(conf);

        setUpHiveServer("src/test/resources/webtrends_event_data.txt", "src/test/resources/webtrends_event.hql",
                "webtrends_event");
    }

    @Test
    public void testJdbcMapper() throws IOException, JSONException {

        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            mapDriver.withInput(NullWritable.get(), record);
        }
        List<Pair<Text, NullWritable>> out = mapDriver.run();
        assertEquals(10, out.size());

        for (Pair<Text, NullWritable> p : out) {
            String jsonData = p.getFirst().toString().split("\t")[1];
            JSONArray json = new JSONArray(jsonData);
            assertNotEquals(0, json.length());
        }
    }

    @Test
    public void testMapReduce() throws IOException {

        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            mapReduceDriver.withInput(NullWritable.get(), record);
        }
        List<Pair<JdbcOutputWritable, NullWritable>> out = mapReduceDriver.run();
        assertEquals(10, out.size());
    }
}
