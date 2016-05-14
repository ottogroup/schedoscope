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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

	private static final Log LOG = LogFactory
			.getLog(HCatToAvroSchemaConverter.class);

	private static final String NAMESPACE = "org.schedoscope.export";

	private static Schema nullSchema = Schema.create(Schema.Type.NULL);

	private Set<String> anonFields;

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

	public HCatToAvroSchemaConverter() {

		this(new HashSet<String>(0));
	}

	public HCatToAvroSchemaConverter(Set<String> anonFields) {

		this.anonFields = anonFields;
	}

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
	public Schema convertSchema(HCatSchema hcatSchema, String tableName)
			throws IOException {

		LOG.info(hcatSchema.getSchemaAsTypeString());
		Schema avroSchema = getRecordAvroFieldSchema(hcatSchema, tableName);
		LOG.info(avroSchema.toString());
		return avroSchema;
	}

	private Schema getRecordAvroFieldSchema(HCatSchema structSchema,
			String fieldName) throws IOException {

		// using jackson 1 is important (org.codehaus)
		// NullNode from jackson 2 doesn't work with avro
		JsonNode n = new ObjectMapper().readTree("null");

		List<Field> fields = new ArrayList<Field>();

		for (HCatFieldSchema f : structSchema.getFields()) {
			if (f.isComplex()) {
				Field complexField = new Field(f.getName(),
						getComplexAvroFieldSchema(f, true), f.getTypeString(),
						n);
				fields.add(complexField);

			} else {
				Field primitiveField = new Field(f.getName(),
						getPrimitiveAvroField(f), f.getTypeString(), n);
				fields.add(primitiveField);
			}
		}
		return Schema.createRecord(fieldName,
				structSchema.getSchemaAsTypeString(), NAMESPACE, false, fields);
	}

	private Schema getComplexAvroFieldSchema(HCatFieldSchema fieldSchema,
			boolean nullable) throws IOException {

		Schema schema = null;
		switch (fieldSchema.getCategory()) {
		case MAP: {
			HCatFieldSchema valueSchema = fieldSchema.getMapValueSchema()
					.get(0);
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
			HCatFieldSchema valueSchema = fieldSchema.getArrayElementSchema()
					.get(0);
			Category valueCategory = valueSchema.getCategory();
			if (valueCategory == Category.PRIMITIVE) {
				Schema subType = getPrimitiveAvroField(valueSchema);
				schema = Schema.createArray(subType);
			} else {
				Schema subType = getComplexAvroFieldSchema(valueSchema, true);
				schema = Schema.createArray(subType);
			}
		}
			break;
		case STRUCT: {
			HCatSchema valueSchema = fieldSchema.getStructSubSchema();
			if (fieldSchema.getName() == null) {
				long hashCode = ((long) fieldSchema.getTypeString().hashCode())
						+ Integer.MAX_VALUE;
				String fieldName = "record_" + String.valueOf(hashCode);
				schema = getRecordAvroFieldSchema(valueSchema, fieldName);
			} else {
				schema = getRecordAvroFieldSchema(valueSchema,
						fieldSchema.getName());
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

	private Schema getPrimitiveAvroField(HCatFieldSchema fieldSchema)
			throws IOException {

		if (primitiveTypeMap.containsKey(fieldSchema.getTypeInfo()
				.getPrimitiveCategory())) {

			Schema schema;
			if (anonFields.contains(fieldSchema.getName())) {
				schema = Schema.create(primitiveTypeMap
						.get(PrimitiveCategory.STRING));
			} else {
				schema = Schema.create(primitiveTypeMap.get(fieldSchema
						.getTypeInfo().getPrimitiveCategory()));
			}
			return Schema.createUnion(ImmutableList.of(nullSchema, schema));
		}
		throw new IllegalArgumentException(
				"can not find primitive type in typeMap");
	}
}
