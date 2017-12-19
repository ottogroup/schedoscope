package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.utils.HCatSchemaTransformer;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.schedoscope.export.utils.HCatSchemaTransformer.transformSchema;

/**
 * Convertor for transforming HCat schemas to BigQuery schemas.
 */
public class HCatSchemaToBigQuerySchemaConverter {

    static private final Log LOG = LogFactory.getLog(HCatSchemaToBigQuerySchemaConverter.class);

    static private final PrimitiveTypeInfo stringTypeInfo = new PrimitiveTypeInfo();

    static {
        stringTypeInfo.setTypeName("string");
    }

    static public final String USED_FILTER_FIELD_NAME = "_USED_HCAT_FILTER";

    static private final Field usedFilterField = Field.newBuilder(USED_FILTER_FIELD_NAME, Field.Type.string()).setMode(Field.Mode.NULLABLE).setDescription("HCatInputFormat filter used to export the present record.").build();

    static private final HCatSchemaTransformer.Constructor<HCatSchema, HCatFieldSchema, Field, Schema> c = new HCatSchemaTransformer.Constructor<HCatSchema, HCatFieldSchema, Field, Schema>() {

        private Field.Type translatePrimitiveType(PrimitiveTypeInfo primitiveTypeInfo) {
            switch (primitiveTypeInfo.getTypeName()) {
                case "int":
                case "bigint":
                case "tinyint":
                    return Field.Type.integer();

                case "boolean":
                    return Field.Type.bool();
                case "float":
                case "double":
                    return Field.Type.floatingPoint();
                default:
                    return Field.Type.string();
            }
        }

        @Override
        public HCatFieldSchema accessPrimitiveField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            return field;
        }

        @Override
        public HCatFieldSchema accessMapField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            return field;
        }

        @Override
        public HCatSchema accessStructField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            try {
                return field.getStructSubSchema();
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public List<HCatFieldSchema> accessPrimitiveArrayField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            return Arrays.asList(field);
        }

        @Override
        public List<HCatFieldSchema> accessArrayArrayField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            return Arrays.asList(field);
        }

        @Override
        public List<HCatFieldSchema> accessMapArrayField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            return Arrays.asList(field);
        }

        @Override
        public List<HCatSchema> accessStructArrayField(HCatSchema schema, HCatFieldSchema field, HCatSchema hCatSchema) {
            try {
                return Arrays.asList(field.getArrayElementSchema().get(0).getStructSubSchema());
            } catch (HCatException e) {
                // not going to happen
                return null;
            }
        }

        @Override
        public Schema constructSchema(List<Field> fields) {
            return Schema.of(fields);
        }

        @Override
        public Field constructPrimitiveField(HCatFieldSchema field, HCatFieldSchema fieldSchema) {
            return Field
                    .newBuilder(field.getName(), translatePrimitiveType(field.getTypeInfo()))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.NULLABLE)
                    .build();
        }

        @Override
        public Field constructMapField(HCatFieldSchema field, HCatFieldSchema fieldSchema) {
            return Field
                    .newBuilder(field.getName(), Field.Type.string())
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.NULLABLE)
                    .build();
        }

        @Override
        public Field constructStructField(HCatSchema schema, HCatFieldSchema field, Schema structSchema) {
            return Field
                    .newBuilder(field.getName(), Field.Type.record(structSchema.getFields()))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.NULLABLE)
                    .build();
        }

        @Override
        public Field constructPrimitiveArrayField(HCatFieldSchema field, PrimitiveTypeInfo elementType, List<HCatFieldSchema> hCatFieldSchemas) {
            return Field
                    .newBuilder(field.getName(), translatePrimitiveType(elementType))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.REPEATED)
                    .build();
        }

        @Override
        public Field constructMapArrayField(HCatFieldSchema field, List<HCatFieldSchema> hCatFieldSchemas) {
            return Field
                    .newBuilder(field.getName(), translatePrimitiveType(stringTypeInfo))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.REPEATED)
                    .build();
        }

        @Override
        public Field constructArrayArrayField(HCatFieldSchema field, List<HCatFieldSchema> hCatFieldSchemas) {
            return Field
                    .newBuilder(field.getName(), translatePrimitiveType(stringTypeInfo))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.REPEATED)
                    .build();
        }

        @Override
        public Field constructStructArrayField(HCatSchema schema, HCatFieldSchema field, List<Schema> schemas) {
            return Field
                    .newBuilder(field.getName(), Field.Type.record(schemas.get(0).getFields()))
                    .setDescription(field.getComment())
                    .setMode(Field.Mode.REPEATED)
                    .build();
        }
    };

    /**
     * Convert a given HCat schema to a BigQuery table definition.
     *
     * @param hcatSchema   the HCat schema to convert
     * @param partitioning should the table be partitioned? If so, with what granularity.
     * @return the BigQuery table definition for a table equivalent to the HCat schema.
     */
    static public TableDefinition convertSchemaToTableDefinition(HCatSchema hcatSchema, PartitioningScheme partitioning) {
        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        List<Field> fields = new LinkedList<>();
        fields.add(usedFilterField);
        fields.addAll(transformSchema(c, hcatSchema).apply(hcatSchema).getFields());

        StandardTableDefinition.Builder tableDefinitionBuilder = StandardTableDefinition
                .newBuilder()
                .setSchema(Schema.of(fields));

        if (partitioning != PartitioningScheme.NONE) {
            tableDefinitionBuilder.setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY));
        }

        TableDefinition tableDefinition = tableDefinitionBuilder.build();

        LOG.info("Converted BigQuery table definition: " + tableDefinition);

        return tableDefinition;
    }

    /**
     * Convert a given HCat schema to a BigQuery table information.
     *
     * @param project      the ID of the GCP project where to create the dataset for the BigQuery table. If null, this is the configured default project.
     * @param dataset      the dataset to create the table in. The dataset will be created if it does not exist yet.
     * @param table        the name of the resulting BigQuery table.
     * @param hcatSchema   the HCat schema to convert
     * @param partitioning should the table be partitioned? If so, with what granularity.
     * @return the BigQuery table info for a table equivalent to the HCat schema.
     * @throws IOException
     */
    static public TableInfo convertSchemaToTableInfo(String project, String dataset, String table, HCatSchema hcatSchema, PartitioningScheme partitioning) throws IOException {

        TableId tableId = project == null ? TableId.of(dataset, table) : TableId.of(project, dataset, table);

        TableInfo tableInfo = TableInfo.of(tableId, convertSchemaToTableDefinition(hcatSchema, partitioning));

        return tableInfo;
    }

    static public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema, PartitioningScheme partitioning) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema, partitioning);
    }

    static public TableInfo convertSchemaToTableInfo(String project, String database, String table, HCatSchema hcatSchema) throws IOException {
        return convertSchemaToTableInfo(project, database, table, hcatSchema, PartitioningScheme.NONE);
    }

    static public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema) throws IOException {
        return convertSchemaToTableInfo(null, database, table, hcatSchema);
    }

}
