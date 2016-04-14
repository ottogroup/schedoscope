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

package org.schedoscope.export.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.JsonSerDe;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The CustomHCatSerializer serializes complex HCatalog types into Json.
 */
public class HCatRecordJsonSerializer {

	private ObjectMapper jsonMapper;

	private JsonSerDe serde;

	/**
	 * The constructor initializes the JsonSerDe and Jackson ObjectMapper.
	 *
	 * @param conf
	 *            The Hadoop configuration object.
	 * @param schema
	 *            The HCatalog Schema
	 */
	public HCatRecordJsonSerializer(Configuration conf, HCatSchema schema) {

		jsonMapper = new ObjectMapper();

		StringBuilder columnNameProperty = new StringBuilder();
		StringBuilder columnTypeProperty = new StringBuilder();

		String prefix = "";
		serde = new JsonSerDe();
		for (HCatFieldSchema f : schema.getFields()) {
			columnNameProperty.append(prefix);
			columnTypeProperty.append(prefix);
			prefix = ",";
			columnNameProperty.append(f.getName());
			columnTypeProperty.append(f.getTypeString());

		}

		Properties tblProps = new Properties();
		tblProps.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperty.toString());
		tblProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypeProperty.toString());
		try {
			serde.initialize(conf, tblProps);
		} catch (SerDeException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	/**
	 * Extracts a complex field as Json from a HCatalog record.
	 *
	 * @param value
	 *            The HCatalogRecord
	 * @param fieldName
	 *            The field to extract.
	 * @return A string representation of the field.
	 * @throws IOException
	 *             Is thrown if an error occurs.
	 */
	public String getFieldAsJson(HCatRecord value, String fieldName) throws IOException {

		String json = null;
		try {
			json = serde.serialize(value, serde.getObjectInspector()).toString();
		} catch (SerDeException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return jsonMapper.readTree(json).get(fieldName).toString();
	}

	/**
	 * Converts a HCatRecord to Json.
	 *
	 * @param value
	 *            The HCatRecord
	 * @return A JsonNode representing the complete HCatRecord.
	 * @throws IOException
	 *             Is thrown if an error occurs
	 */
	public JsonNode getRecordAsJson(HCatRecord value) throws IOException {

		String json = null;
		try {
			json = serde.serialize(value, serde.getObjectInspector()).toString();
		} catch (SerDeException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		return jsonMapper.readTree(json);
	}
}
