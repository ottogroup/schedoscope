package org.schedoscope.export.bigquery.outputschema;


import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.util.LinkedList;
import java.util.List;

public abstract class HCatSchemaConvertor<T, F, S> {

    static private PrimitiveTypeInfo stringTypeInfo;

    private PrimitiveTypeInfo stringTypeInfo() {
        if (stringTypeInfo == null) {
            stringTypeInfo = new PrimitiveTypeInfo();
            stringTypeInfo.setTypeName("string");
        }

        return stringTypeInfo;
    }

    protected abstract T createPrimitiveSchemaField(PrimitiveTypeInfo typeInfo);

    protected abstract F createPrimitiveArrayField(HCatFieldSchema fieldSchema, PrimitiveTypeInfo elementSchema);

    protected abstract F createStructArrayField(HCatFieldSchema fieldSchema, HCatSchema subSchema);

    protected abstract F createStructSchemaField(HCatFieldSchema fieldSchema, S recordSchema);

    protected abstract S createSchema(List<F> fields);

    protected abstract F convertPrimitiveSchemaField(HCatFieldSchema fieldSchema);

    public F convertStructSchemaField(HCatFieldSchema fieldSchema) {

        HCatSchema structSchema = null;

        try {
            structSchema = fieldSchema.getStructSubSchema();
        } catch (HCatException e) {
            // not going to happen
        }

        return createStructSchemaField(fieldSchema, convertSchemaFields(structSchema));

    }

    public F convertArraySchemaField(HCatFieldSchema fieldSchema) {

        HCatFieldSchema elementSchema = null;

        try {
            elementSchema = fieldSchema.getArrayElementSchema().get(0);
        } catch (HCatException e) {
            // not going to happen
        }

        if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())
            return createPrimitiveArrayField(fieldSchema, elementSchema.getTypeInfo());
        else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())
            return createPrimitiveArrayField(fieldSchema, stringTypeInfo());
        else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())
            return createPrimitiveArrayField(fieldSchema, stringTypeInfo());
        else
            try {
                return createStructArrayField(fieldSchema, elementSchema.getStructSubSchema());
            } catch (HCatException e) {
                return null; // not going to happen
            }

    }

    public F convertSchemaField(HCatFieldSchema fieldSchema) {

        if (HCatFieldSchema.Category.ARRAY == fieldSchema.getCategory())
            return convertArraySchemaField(fieldSchema);
        else if (HCatFieldSchema.Category.STRUCT == fieldSchema.getCategory())
            return convertStructSchemaField(fieldSchema);
        else if (HCatFieldSchema.Category.MAP == fieldSchema.getCategory())
            try {
                return convertPrimitiveSchemaField(new HCatFieldSchema(fieldSchema.getName(), stringTypeInfo(), fieldSchema.getComment()));
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        else
            return convertPrimitiveSchemaField(fieldSchema);

    }

    public S convertSchemaFields(HCatSchema hcatSchema) {
        LinkedList<F> convertedFields = new LinkedList<>();

        for (HCatFieldSchema field : hcatSchema.getFields()) {
            convertedFields.add(convertSchemaField(field));
        }

        return createSchema(convertedFields);
    }
}
