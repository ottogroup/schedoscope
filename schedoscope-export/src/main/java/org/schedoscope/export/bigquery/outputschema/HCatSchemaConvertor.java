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

    protected abstract T constructPrimitiveType(PrimitiveTypeInfo typeInfo);

    protected abstract F constructPrimitiveArrayField(HCatFieldSchema fieldSchema, T elementType);

    protected abstract F constructPrimitiveField(HCatFieldSchema fieldSchema, T fieldType);

    protected abstract F constructStructArrayField(HCatFieldSchema fieldSchema, HCatSchema subSchema);

    protected abstract F constructStructField(HCatFieldSchema fieldSchema, S recordSchema);

    protected abstract S constructSchema(List<F> fields);


    public F convertStructSchemaField(HCatFieldSchema fieldSchema) {

        HCatSchema structSchema = null;

        try {
            structSchema = fieldSchema.getStructSubSchema();
        } catch (HCatException e) {
            // not going to happen
        }

        return constructStructField(fieldSchema, convertSchemaFields(structSchema));

    }

    public F convertArraySchemaField(HCatFieldSchema fieldSchema) {

        HCatFieldSchema elementSchema = null;

        try {
            elementSchema = fieldSchema.getArrayElementSchema().get(0);
        } catch (HCatException e) {
            // not going to happen
        }

        if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())
            return constructPrimitiveArrayField(fieldSchema, constructPrimitiveType(elementSchema.getTypeInfo()));
        else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())
            return constructPrimitiveArrayField(fieldSchema, constructPrimitiveType(stringTypeInfo()));
        else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())
            return constructPrimitiveArrayField(fieldSchema, constructPrimitiveType(stringTypeInfo()));
        else
            try {
                return constructStructArrayField(fieldSchema, elementSchema.getStructSubSchema());
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
                return constructPrimitiveField(new HCatFieldSchema(fieldSchema.getName(), stringTypeInfo(), fieldSchema.getComment()), constructPrimitiveType(stringTypeInfo()));
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        else
            return constructPrimitiveField(fieldSchema, constructPrimitiveType(fieldSchema.getTypeInfo()));

    }

    public S convertSchemaFields(HCatSchema hcatSchema) {
        LinkedList<F> convertedFields = new LinkedList<>();

        for (HCatFieldSchema field : hcatSchema.getFields()) {
            convertedFields.add(convertSchemaField(field));
        }

        return constructSchema(convertedFields);
    }
}
