package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.util.LinkedList;

public class BigQuerySchema {

    private static final Log LOG = LogFactory.getLog(BigQuerySchema.class);

    public Field.Type convertTypeInfoToFieldType(PrimitiveTypeInfo typeInfo) {
        Field.Type bigQueryType;

        switch (typeInfo.getTypeName()) {
            case "string":
                bigQueryType = Field.Type.string();
                break;
            case "int":
                bigQueryType = Field.Type.integer();
                break;
            case "bigint":
                bigQueryType = Field.Type.integer();
                break;
            case "tinyint":
                bigQueryType = Field.Type.integer();
                break;
            case "boolean":
                bigQueryType = Field.Type.bool();
                break;
            case "float":
                bigQueryType = Field.Type.floatingPoint();
                break;
            case "double":
                bigQueryType = Field.Type.floatingPoint();
                break;
            default:
                bigQueryType = Field.Type.string();
        }

        return bigQueryType;
    }

    public Field convertFieldSchemaToField(HCatFieldSchema fieldSchema) {

        String fieldName = fieldSchema.getName();
        String fieldDescription = fieldSchema.getComment();

        PrimitiveTypeInfo fieldType;
        Field.Mode mode = Field.Mode.NULLABLE;

        if (fieldSchema.getCategory().equals(HCatFieldSchema.Category.ARRAY)) {

            HCatFieldSchema elementSchema = null;

            try {
                elementSchema = fieldSchema.getArrayElementSchema().get(0);
            } catch (HCatException e) {
                // not going to happen
            }

            if (elementSchema.getCategory().equals(HCatFieldSchema.Category.PRIMITIVE)) {

                mode = Field.Mode.REPEATED;
                fieldType = elementSchema.getTypeInfo();

            } else {

                fieldType = elementSchema.getTypeInfo();

            }

        } else {
            fieldType = fieldSchema.getTypeInfo();
        }

        Field.Type bigQueryType = convertTypeInfoToFieldType(fieldType);

        return Field
                .newBuilder(fieldName, bigQueryType)
                .setDescription(fieldDescription)
                .setMode(mode)
                .build();

    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema) throws IOException {

        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        TableId tableId = TableId.of(database, table);

        LinkedList<Field> biqQueryFields = new LinkedList<>();

        for (HCatFieldSchema field : hcatSchema.getFields()) {
            biqQueryFields.add(convertFieldSchemaToField(field));
        }

        Schema tableFields = Schema.of(biqQueryFields);

        StandardTableDefinition tableDefinition = StandardTableDefinition.of(tableFields);

        TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);

        LOG.info("Converted BigQuery schema: " + tableInfo);

        return tableInfo;
    }

}
