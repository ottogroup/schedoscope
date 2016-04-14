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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.schedoscope.export.utils.HCatRecordJsonSerializer;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * This class converts an HCatRecord to an AvroRecord. It uses the
 * Avro schema to recursively construct the record, requesting the
 * fields from the JsonNode as needed.
 */
public class HCatToAvroRecordConverter {

	private HCatRecordJsonSerializer serializer;

	public HCatToAvroRecordConverter(HCatRecordJsonSerializer serializer) {

		this.serializer = serializer;
	}

	/**
	 * This function converts an HCatRecord to an Avro
	 * GenericRecord.
	 *
	 * @param hcatRecord The HCatRecord
	 * @param avroSchema The Avro Schema
	 * @return Returns an Avro GenericRecord
	 * @throws IOException Is thrown if an error occurs
	 */
	public GenericRecord convert(HCatRecord hcatRecord, Schema avroSchema) throws IOException {

		JsonNode json = serializer.getRecordAsJson(hcatRecord);
		GenericRecord avroRecord = convertRecord(json, avroSchema);
		return avroRecord;
	}

	private GenericRecord convertRecord(JsonNode json, Schema schema) throws IOException {

		GenericRecordBuilder builder = new GenericRecordBuilder(schema);

		List<Field> fields = schema.getFields();
		for (Field f : fields) {

			for (Schema s : f.schema().getTypes()) {
				if (s.getType().equals(Schema.Type.STRING)) {
					builder.set(f.name(), json.get(f.name()).asText());
				} else if (s.getType().equals(Schema.Type.INT)) {
					builder.set(f.name(), json.get(f.name()).asInt());
				} else if (s.getType().equals(Schema.Type.LONG)) {
					builder.set(f.name(), json.get(f.name()).asLong());
				} else if (s.getType().equals(Schema.Type.BOOLEAN)) {
					builder.set(f.name(), json.get(f.name()).asBoolean());
				} else if (s.getType().equals(Schema.Type.DOUBLE)) {
					builder.set(f.name(), json.get(f.name()).asDouble());
				} else if (s.getType().equals(Schema.Type.FLOAT)) {
					builder.set(f.name(), json.get(f.name()).asDouble());
				} else if (s.getType().equals(Schema.Type.RECORD)) {
					builder.set(f.name(), json.get(f.name()));
				} else if (s.getType().equals(Schema.Type.ARRAY)) {
					builder.set(f.name(), convertArray(json.get(f.name()), s));
				} else if (s.getType().equals(Schema.Type.MAP)) {
					builder.set(f.name(), convertMap(json.get(f.name()), s));
				}
			}
		}
		return builder.build();
	}

	private Map<String, Object> convertMap(JsonNode json, Schema schema) throws IOException {

		Map<String, Object> res = new HashMap<>();

		Iterator<Map.Entry<String, JsonNode>> it = json.fields();

		while (it.hasNext()) {

			Map.Entry<String, JsonNode> n = it.next();
			for (Schema s : schema.getValueType().getTypes()) {
				if (s.getType().equals(Schema.Type.STRING)) {
					res.put(n.getKey(), n.getValue().asText());
				} else if (s.getType().equals(Schema.Type.INT)) {
					res.put(n.getKey(), n.getValue().asInt());
				} else if (s.getType().equals(Schema.Type.LONG)) {
					res.put(n.getKey(), n.getValue().asLong());
				} else if (s.getType().equals(Schema.Type.BOOLEAN)) {
					res.put(n.getKey(), n.getValue().asBoolean());
				} else if (s.getType().equals(Schema.Type.DOUBLE)) {
					res.put(n.getKey(), n.getValue().asDouble());
				} else if (s.getType().equals(Schema.Type.FLOAT)) {
					res.put(n.getKey(), n.getValue().asDouble());
				} else if (s.getType().equals(Schema.Type.RECORD)) {
					res.put(n.getKey(), convertRecord(n.getValue(), s));
				} else if (s.getType().equals(Schema.Type.ARRAY)) {
					res.put(n.getKey(), convertArray(n.getValue(), s));
				} else if (s.getType().equals(Schema.Type.MAP)) {
					res.put(n.getKey(), convertMap(n.getValue(), s));
				}
			}
		}
		return res;
	}

	private List<Object> convertArray(JsonNode json, Schema schema) throws IOException {

		List<Object> res = new ArrayList<>();
		Iterator<JsonNode> it = json.elements();

		while (it.hasNext()) {

			JsonNode n = it.next();
			for (Schema s : schema.getElementType().getTypes()) {
				if (s.getType().equals(Schema.Type.STRING)) {
					res.add(n.asText());
				} else if (s.getType().equals(Schema.Type.INT)) {
					res.add(n.asInt());
				} else if (s.getType().equals(Schema.Type.LONG)) {
					res.add(n.asLong());
				} else if (s.getType().equals(Schema.Type.BOOLEAN)) {
					res.add(n.asBoolean());
				} else if (s.getType().equals(Schema.Type.DOUBLE)) {
					res.add(n.asDouble());
				} else if (s.getType().equals(Schema.Type.FLOAT)) {
					res.add(n.asDouble());
				} else if (s.getType().equals(Schema.Type.RECORD)) {
					res.add(convertRecord(n, s));
				} else if (s.getType().equals(Schema.Type.ARRAY)) {
					res.addAll(convertArray(n, s));
				} else if (s.getType().equals(Schema.Type.MAP)) {
					res.add(convertMap(n, s));
				}
			}
		}
		return res;
	}
}
