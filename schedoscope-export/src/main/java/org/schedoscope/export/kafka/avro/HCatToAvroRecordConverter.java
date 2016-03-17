package org.schedoscope.export.kafka.avro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Category;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import com.google.common.collect.ImmutableList;

public class HCatToAvroRecordConverter {

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

    public GenericRecord convert(HCatRecord record, HCatSchema hcatSchema) throws HCatException {

        return getRecordValue(hcatSchema, "my_table", record);
    }

    private GenericRecord getRecordValue(HCatSchema structSchema, String fieldName, HCatRecord record)
            throws HCatException {

        List<Pair<String, Object>> values = new ArrayList<Pair<String, Object>>();
        List<Field> fields = new ArrayList<Field>();

        for (HCatFieldSchema f : structSchema.getFields()) {
            if (f.isComplex()) {
                Field complexField = new Field(f.getName(), getComplexAvroFieldSchema(f), f.getTypeString(), false);
                fields.add(complexField);
                values.add(Pair.of(f.getName(), record.get(f.getName(), structSchema)));
            } else {
                Field primitiveField = new Field(f.getName(), getPrimitiveAvroField(f), f.getTypeString(), false);
                fields.add(primitiveField);
                values.add(Pair.of(f.getName(), record.get(f.getName(), structSchema)));
            }
        }
        Schema schema = Schema.createRecord(fieldName, structSchema.getSchemaAsTypeString(), NAMESPACE, false, fields);
        GenericRecord rec = new GenericData.Record(schema);

        for (Pair<String, Object> v : values) {
            rec.put(v.getKey(), v.getValue());
        }
        return rec;
    }

    private Schema getRecordAvroFieldSchema(HCatSchema structSchema, String fieldName) throws HCatException {

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

    private Schema getComplexAvroFieldSchema(HCatFieldSchema fieldSchema) throws HCatException {

        Schema schema = null;
        switch (fieldSchema.getCategory()) {
        case MAP:
        {
            HCatFieldSchema valueSchema = fieldSchema.getMapValueSchema().get(0);
            Category valueCategory = valueSchema.getCategory();
            if (valueCategory == Category.PRIMITIVE) {
                Schema subType = getPrimitiveAvroField(valueSchema);
                schema = Schema.createMap(subType);
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

        return Schema.createUnion(ImmutableList.of(schema, nullSchema));
    }

    private Schema getPrimitiveAvroField(HCatFieldSchema fieldSchema) throws HCatException {

        if (primitiveTypeMap.containsKey(fieldSchema.getTypeInfo().getPrimitiveCategory())) {
            Schema schema = Schema.create(primitiveTypeMap.get(fieldSchema.getTypeInfo().getPrimitiveCategory()));
            return Schema.createUnion(ImmutableList.of(schema, nullSchema));
        }
        throw new IllegalArgumentException("can not find primitive type in typeMap");
    }
}
