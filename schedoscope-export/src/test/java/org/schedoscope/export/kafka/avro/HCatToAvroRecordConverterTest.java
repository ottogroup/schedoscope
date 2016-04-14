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

package org.schedoscope.export.kafka.avro;

import static org.junit.Assert.assertNotNull;

import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.utils.CustomHCatRecordSerializer;

import com.fasterxml.jackson.databind.JsonNode;

public class HCatToAvroRecordConverterTest extends HiveUnitBaseTest {

	@Override
	@Before
	public void setUp() throws Exception {

		super.setUp();
	}

	@Test
	public void testMapConverter() throws Exception {

		setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}

	@Test
	public void testArrayConverter() throws Exception {

		setUpHiveServer("src/test/resources/test_array_data.txt", "src/test/resources/test_array.hql", "test_array");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}

	@Test
	public void testStructConverter() throws Exception {

		setUpHiveServer("src/test/resources/test_struct_data.txt", "src/test/resources/test_struct.hql", "test_struct");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}

	@Test
	public void testMapArrayConverter() throws Exception {

		setUpHiveServer("src/test/resources/test_maparray_data.txt", "src/test/resources/test_maparray.hql",
				"test_maparray");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}

	@Test
	public void testStructStructConverter() throws Exception {

		setUpHiveServer("src/test/resources/test_structstruct_data.txt", "src/test/resources/test_structstruct.hql",
				"test_structstruct");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}

	@Test
	public void testArrayStructConverter() throws Exception {
		setUpHiveServer("src/test/resources/test_arraystruct_data.txt", "src/test/resources/test_arraystruct.hql",
				"test_arraystruct");

		CustomHCatRecordSerializer serializer = new CustomHCatRecordSerializer(conf, hcatInputSchema);
		Schema schema = HCatToAvroSchemaConverter.convertSchema(hcatInputSchema, "MyRecord");

		Iterator<HCatRecord> it = hcatRecordReader.read();
		while (it.hasNext()) {

			HCatRecord record = it.next();
			JsonNode json = serializer.getAsJson(record);
			GenericRecord rec = HCatToAvroRecordConverter.convertRecord(json, schema);
			assertNotNull(rec);
		}
	}
}
