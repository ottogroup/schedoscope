package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HCatSchemaToBigQuerySchemaConverter extends HCatSchemaConvertor<Field.Type, Field, Schema> {

    private static final Log LOG = LogFactory.getLog(HCatSchemaToBigQuerySchemaConverter.class);

    private Field usedFilterField = Field.newBuilder("_USED_HCAT_FILTER", Field.Type.string()).setMode(Field.Mode.NULLABLE).setDescription("HCatInputFormat filter used to export the present record.").build();



    @Override
    protected Field constructStructField(HCatFieldSchema fieldSchema, Schema recordSchema) {
        return Field
                .newBuilder(fieldSchema.getName(), Field.Type.record(recordSchema.getFields()))
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.NULLABLE)
                .build();
    }


    @Override
    protected Field constructStructArrayField(HCatFieldSchema fieldSchema, HCatSchema subSchema) {
        return Field
                .newBuilder(fieldSchema.getName(), Field.Type.record(convertSchemaFields(subSchema).getFields()))
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.REPEATED)
                .build();
    }


    @Override
    protected Schema constructSchema(List<Field> fields) {
        return Schema.of(fields);
    }

    @Override
    public Field.Type constructPrimitiveType(PrimitiveTypeInfo typeInfo) {
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

    @Override
    protected Field constructPrimitiveArrayField(HCatFieldSchema fieldSchema, Field.Type elementType) {
        return Field
                .newBuilder(fieldSchema.getName(), elementType)
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.REPEATED)
                .build();
    }

    @Override
    protected Field constructPrimitiveField(HCatFieldSchema fieldSchema, Field.Type fieldType) {
        return Field
                .newBuilder(fieldSchema.getName(), fieldType)
                .setDescription(fieldSchema.getComment())
                .setMode(Field.Mode.NULLABLE)
                .build();
    }

    public TableDefinition convertSchemaToTableDefinition(HCatSchema hcatSchema, PartitioningScheme partitioning) {
        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        List<Field> fields = new LinkedList<>();
        fields.add(usedFilterField);
        fields.addAll(convertSchemaFields(hcatSchema).getFields());

        StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(fields));

        if (partitioning.isTemporallyPartitioned()) {
            tableDefinitionBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
        }

        TableDefinition tableDefinition = tableDefinitionBuilder.build();

        LOG.info("Converted BigQuery table definition: " + tableDefinition);

        return tableDefinition;
    }

    public TableInfo convertSchemaToTableInfo(String project, String database, String table, HCatSchema hcatSchema, PartitioningScheme partitioning, String postfix) throws IOException {

        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        TableId tableId = project == null ? TableId.of(database, table + (postfix == null || postfix.isEmpty() ? "" : "_" + postfix)) : TableId.of(project, database, table + (postfix == null || postfix.isEmpty() ? "" : "_" + postfix));

        TableInfo tableInfo = TableInfo.of(tableId, convertSchemaToTableDefinition(hcatSchema, partitioning));

        LOG.info("Converted BigQuery schema: " + tableInfo);

        return tableInfo;
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema, PartitioningScheme partitioning, String postfix) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema, partitioning, postfix);
    }


    public TableInfo convertSchemaToTableInfo(String project, String database, String table, HCatSchema hcatSchema, PartitioningScheme partitioning) throws IOException {
        return convertSchemaToTableInfo(project, database, table, hcatSchema, partitioning, "");
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema, PartitioningScheme partitioning) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema, partitioning);
    }

    public TableInfo convertSchemaToTableInfo(String project, String database, String table, HCatSchema hcatSchema, String postfix) throws IOException {
        return convertSchemaToTableInfo(project, database, table, hcatSchema, new PartitioningScheme(), postfix);
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema, String postfix) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema, postfix);
    }

    public TableInfo convertSchemaToTableInfo(String project, String database, String table, HCatSchema hcatSchema) throws IOException {
        return convertSchemaToTableInfo(project, database, table, hcatSchema, new PartitioningScheme());
    }

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema);
    }

}
