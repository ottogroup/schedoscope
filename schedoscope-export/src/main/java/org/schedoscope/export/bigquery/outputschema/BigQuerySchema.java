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
import java.util.List;

public class BigQuerySchema {

    private static final Log LOG = LogFactory.getLog(BigQuerySchema.class);

    static private PrimitiveTypeInfo stringTypeInfo;

    private PrimitiveTypeInfo stringTypeInfo() {
        if (stringTypeInfo == null) {
            stringTypeInfo = new PrimitiveTypeInfo();
            stringTypeInfo.setTypeName("string");
        }

        return stringTypeInfo;
    }

    private Field usedFilterField = Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setMode(Field.Mode.NULLABLE).setDescription("HCatInputFormat filter used to export the present record.").build();

    private Field.Type convertPrimitiveTypeInfoToFieldType(PrimitiveTypeInfo typeInfo) {
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

    private Field convertPrimitiveSchemaToField(HCatFieldSchema fieldSchema) {

        return Field
                .newBuilder(fieldSchema.getName(), convertPrimitiveTypeInfoToFieldType(fieldSchema.getTypeInfo()))
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.NULLABLE)
                .build();

    }

    private Field convertStructSchemaField(HCatFieldSchema fieldSchema) {

        HCatSchema structSchema = null;

        try {
            structSchema = fieldSchema.getStructSubSchema();
        } catch (HCatException e) {
            // not going to happen
        }

        Schema recordSchema = convertSchemaToTableFields(structSchema);

        return Field
                .newBuilder(fieldSchema.getName(), Field.Type.record(recordSchema.getFields()))
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.NULLABLE)
                .build();

    }

    private Field convertArraySchemaField(HCatFieldSchema fieldSchema) {

        HCatFieldSchema elementSchema = null;

        try {
            elementSchema = fieldSchema.getArrayElementSchema().get(0);
        } catch (HCatException e) {
            // not going to happen
        }

        Field.Type arrayFieldType = null;

        if (HCatFieldSchema.Category.PRIMITIVE == elementSchema.getCategory())
            arrayFieldType = convertPrimitiveTypeInfoToFieldType(elementSchema.getTypeInfo());
        else if (HCatFieldSchema.Category.ARRAY == elementSchema.getCategory())
            arrayFieldType = Field.Type.string();
        else if (HCatFieldSchema.Category.MAP == elementSchema.getCategory())
            arrayFieldType = Field.Type.string();
        else
            try {
                arrayFieldType = Field.Type.record(convertSchemaToTableFields(elementSchema.getStructSubSchema()).getFields());
            } catch (HCatException e) {
                // not going to happen
            }


        return Field
                .newBuilder(fieldSchema.getName(), arrayFieldType)
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.REPEATED)
                .build();

    }

    private Field convertFieldSchemaToField(HCatFieldSchema fieldSchema) {

        if (HCatFieldSchema.Category.ARRAY == fieldSchema.getCategory())
            return convertArraySchemaField(fieldSchema);
        else if (HCatFieldSchema.Category.STRUCT == fieldSchema.getCategory())
            return convertStructSchemaField(fieldSchema);
        else if (HCatFieldSchema.Category.MAP == fieldSchema.getCategory())
            try {
                return convertPrimitiveSchemaToField(new HCatFieldSchema(fieldSchema.getName(), stringTypeInfo(), fieldSchema.getComment()));
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        else
            return convertPrimitiveSchemaToField(fieldSchema);

    }

    private Schema convertSchemaToTableFields(HCatSchema hcatSchema) {
        LinkedList<Field> biqQueryFields = new LinkedList<>();

        for (HCatFieldSchema field : hcatSchema.getFields()) {
            biqQueryFields.add(convertFieldSchemaToField(field));
        }

        return Schema.of(biqQueryFields);
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema, TemporalPartitioningScheme partitioning) throws IOException {

        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        TableId tableId = TableId.of(database, table);

        List<Field> fields = new LinkedList<>();
        fields.add(usedFilterField);
        fields.addAll(convertSchemaToTableFields(hcatSchema).getFields());

        StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(fields));

        if (partitioning.isDefined()) {
            tableDefinitionBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
        }

        TableInfo tableInfo = TableInfo.of(tableId, tableDefinitionBuilder.build());

        LOG.info("Converted BigQuery schema: " + tableInfo);

        return tableInfo;
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema) throws IOException {
        return convertSchemaToTableInfo(database, table, hcatSchema, new TemporalPartitioningScheme());
    }

}
