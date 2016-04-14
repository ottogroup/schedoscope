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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Category;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.ImmutableList;

/**
 * This class converts an HCatRecord schema to an AvroRecord schema.
 */
public class HCatToAvroSchemaConverter {

	private static final Log LOG = LogFactory.getLog(HCatToAvroSchemaConverter.class);

	private static final String NAMESPACE = "org.schedoscope.export";

	private static Schema nullSchema = Schema.create(Schema.Type.NULL);

	@SuppressWarnings("serial")
	private static final Map<PrimitiveCategory, Schema.Type> primitiveTypeMap = Collections
			.unmodifiableMap(new HashMap<PrimitiveCategory, Schema.Type>() {
				{
					put(PrimitiveCategory.BOOLEAN, Schema.Type.BOOLEAN);
					put(PrimitiveCategory.INT, Schema.Type.INT);
					put(PrimitiveCategory.STRING, Schema.Type.STRING);
					put(PrimitiveCategory.LONG, Schema.Type.LONG);
					put(PrimitiveCategory.VARCHAR, Schema.Type.STRING);
					put(PrimitiveCategory.FLOAT, Schema.Type.FLOAT);
					put(PrimitiveCategory.DOUBLE, Schema.Type.DOUBLE);
					put(PrimitiveCategory.BYTE, Schema.Type.INT);
				}
			});

	/**
	 * Converts a given HCatrecord to a GeneircRecord (Avro).
	 *
	 * @param record
	 *            The HCatRecord.
	 * @param hcatSchema
	 *            The HCatSchema.
	 * @param tableName
	 *            The Hive table name, will be the name of the GenericRecord.
	 * @return A GenericRecord..
	 * @throws HCatException
	 *             Is thrown if an error occurs.
	 */
//	public static GenericRecord convertRecord(HCatRecord record, HCatSchema hcatSchema, String tableName)
//			throws IOException {
//
//		LOG.info(record.toString());
//		GenericRecord rec = getRecordValue(hcatSchema, tableName, record);
//		LOG.info(rec.toString());
//		LOG.info(rec.getSchema());
//		return rec;
//	}

	/**
	 * Converts a HCatSchema to an Avro Schema.
	 *
	 * @param hcatSchema
	 *            The HCatSchema.
	 * @param tableName
	 *            The name of the table, will be the record name.
	 * @return A derived Avro Schema.
	 * @throws HCatException
	 *             Is thrown if an error occurs.
	 */
	public static Schema convertSchema(HCatSchema hcatSchema, String tableName) throws IOException {

		LOG.info(hcatSchema.getSchemaAsTypeString());
		Schema avroSchema = getRecordAvroFieldSchema(hcatSchema, tableName);
		LOG.info(avroSchema.toString());
		return avroSchema;
	}

//	private static GenericRecord getRecordValue(HCatSchema structSchema, String fieldName, HCatRecord record)
//			throws IOException {
//
//		JsonNode nullNode = new ObjectMapper().readTree("null");
//
//		List<Pair<String, Object>> values = new ArrayList<Pair<String, Object>>();
//		List<Field> fields = new ArrayList<Field>();
//
//		for (HCatFieldSchema f : structSchema.getFields()) {
//			if (f.isComplex()) {
//				Field complexField = new Field(f.getName(), getComplexAvroFieldSchema(f, true), f.getTypeString(),
//						nullNode);
//				fields.add(complexField);
//				values.add(Pair.of(f.getName(), record.get(f.getName(), structSchema)));
//			} else {
//				Field primitiveField = new Field(f.getName(), getPrimitiveAvroField(f), f.getTypeString(), nullNode);
//				fields.add(primitiveField);
//				values.add(Pair.of(f.getName(), record.get(f.getName(), structSchema)));
//			}
//		}
//		Schema schema = Schema.createRecord(fieldName, structSchema.getSchemaAsTypeString(), NAMESPACE, false, fields);
//		GenericRecord rec = new GenericData.Record(schema);
//
//		for (Pair<String, Object> v : values) {
//			rec.put(v.getKey(), v.getValue());
//		}
//		return rec;
//	}

	private static Schema getRecordAvroFieldSchema(HCatSchema structSchema, String fieldName) throws IOException {

		JsonNode n = new ObjectMapper().readTree("null");

		List<Field> fields = new ArrayList<Field>();

		for (HCatFieldSchema f : structSchema.getFields()) {
			if (f.isComplex()) {
				Field complexField = new Field(f.getName(), getComplexAvroFieldSchema(f, true), f.getTypeString(), n);
				fields.add(complexField);

			} else {
				Field primitiveField = new Field(f.getName(), getPrimitiveAvroField(f), f.getTypeString(), n);
				fields.add(primitiveField);
			}
		}
		return Schema.createRecord(fieldName, structSchema.getSchemaAsTypeString(), NAMESPACE, false, fields);
	}

	private static Schema getComplexAvroFieldSchema(HCatFieldSchema fieldSchema, boolean nullable) throws IOException {

		Schema schema = null;
		switch (fieldSchema.getCategory()) {
		case MAP: {
			HCatFieldSchema valueSchema = fieldSchema.getMapValueSchema().get(0);
			Category valueCategory = valueSchema.getCategory();
			if (valueCategory == Category.PRIMITIVE) {
				Schema subType = getPrimitiveAvroField(valueSchema);
				schema = Schema.createMap(subType);
			} else {
				Schema subType = getComplexAvroFieldSchema(valueSchema, true);
				schema = Schema.createMap(subType);
			}
		}
			break;
		case ARRAY: {
			HCatFieldSchema valueSchema = fieldSchema.getArrayElementSchema().get(0);
			Category valueCategory = valueSchema.getCategory();
			if (valueCategory == Category.PRIMITIVE) {
				Schema subType = getPrimitiveAvroField(valueSchema);
				schema = Schema.createArray(subType);
				// } else if (valueCategory == Category.STRUCT) {
				// Schema subType = getComplexAvroFieldSchema(valueSchema,
				// false);
				// schema = Schema.createArray(subType);
			} else {
				Schema subType = getComplexAvroFieldSchema(valueSchema, true);
				schema = Schema.createArray(subType);
			}
		}
			break;
		case STRUCT: {
			HCatSchema valueSchema = fieldSchema.getStructSubSchema();
			if (fieldSchema.getName() == null) {
				long hashCode = ((long) fieldSchema.getTypeString().hashCode()) + Integer.MAX_VALUE;
				String fieldName = "record_" + String.valueOf(hashCode);
				schema = getRecordAvroFieldSchema(valueSchema, fieldName);
			} else {
				schema = getRecordAvroFieldSchema(valueSchema, fieldSchema.getName());
			}
		}
			break;
		default:
			throw new IllegalArgumentException("invalid type");
		}

		if (nullable) {
			return Schema.createUnion(ImmutableList.of(nullSchema, schema));
		} else {
			return schema;
		}
	}

	private static Schema getPrimitiveAvroField(HCatFieldSchema fieldSchema) throws IOException {

		if (primitiveTypeMap.containsKey(fieldSchema.getTypeInfo().getPrimitiveCategory())) {
			Schema schema = Schema.create(primitiveTypeMap.get(fieldSchema.getTypeInfo().getPrimitiveCategory()));
			return Schema.createUnion(ImmutableList.of(nullSchema, schema));
		}
		throw new IllegalArgumentException("can not find primitive type in typeMap");
	}
}
