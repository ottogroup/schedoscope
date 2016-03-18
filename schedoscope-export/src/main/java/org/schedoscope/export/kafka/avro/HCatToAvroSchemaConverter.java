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

import com.google.common.collect.ImmutableList;

/**
 * A class to convert a HCatSchema to an Avro Schema.
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
                }
            });

    /**
     * Function converts the given HCatSchema to an Avro schema.
     *
     * @return An Avro schema representing the HCatSchema.
     */
    public static Schema convert(HCatSchema hcatSchema, String tableName) throws HCatException {

        LOG.info(hcatSchema.getSchemaAsTypeString());
        Schema avroSchema = getRecordAvroFieldSchema(hcatSchema, tableName);
        LOG.info(avroSchema.toString());
        return avroSchema;
    }

    private static Schema getRecordAvroFieldSchema(HCatSchema structSchema, String fieldName) throws HCatException {

        List<Field> fields = new ArrayList<Field>();

        for (HCatFieldSchema f : structSchema.getFields()) {
            if (f.isComplex()) {
                Field complexField = new Field(f.getName(), getComplexAvroFieldSchema(f), f.getTypeString(), false);
                fields.add(complexField);

            } else {
                Field primitiveField = new Field(f.getName(), getPrimitiveAvroField(f), f.getTypeString(), false);
                fields.add(primitiveField);
            }
        }

        return Schema.createRecord(fieldName, structSchema.getSchemaAsTypeString(), NAMESPACE, false, fields);
    }

    private static Schema getComplexAvroFieldSchema(HCatFieldSchema fieldSchema) throws HCatException {

        Schema schema = null;
        switch (fieldSchema.getCategory()) {
        case MAP:
        {
            HCatFieldSchema valueSchema = fieldSchema.getMapValueSchema().get(0);
            Category valueCategory = valueSchema.getCategory();
            if (valueCategory == Category.PRIMITIVE) {
                Schema subType = getPrimitiveAvroField(valueSchema);
                schema = Schema.createMap(subType);
            } else {
                schema = getComplexAvroFieldSchema(valueSchema);
            }
        }
            break;
        case ARRAY:
        {
            HCatFieldSchema valueSchema = fieldSchema.getArrayElementSchema().get(0);
            Category valueCategory = valueSchema.getCategory();
            if (valueCategory == Category.PRIMITIVE) {
                Schema subType = getPrimitiveAvroField(valueSchema);
                schema = Schema.createArray(subType);
            } else {
                schema = getComplexAvroFieldSchema(valueSchema);
            }
        }
            break;
        case STRUCT:
        {
            HCatSchema valueSchema = fieldSchema.getStructSubSchema();
            schema = getRecordAvroFieldSchema(valueSchema, fieldSchema.getName());
        }
            break;
        default:
            throw new IllegalArgumentException("invalid type");
        }

        return schema; //Schema.createUnion(ImmutableList.of(schema, nullSchema));
    }

    private static Schema getPrimitiveAvroField(HCatFieldSchema fieldSchema) {

        if (primitiveTypeMap.containsKey(fieldSchema.getTypeInfo().getPrimitiveCategory())) {
            Schema schema = Schema.create(primitiveTypeMap.get(fieldSchema.getTypeInfo().getPrimitiveCategory()));
            //schema.getJsonProps();
            return Schema.createUnion(ImmutableList.of(nullSchema, schema));
        }
        throw new IllegalArgumentException("can not find primitive type in typeMap");
    }
}
