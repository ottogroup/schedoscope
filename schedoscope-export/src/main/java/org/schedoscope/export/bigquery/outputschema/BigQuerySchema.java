package org.schedoscope.export.bigquery.outputschema;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

import java.io.IOException;

public class BigQuerySchema {

    private static final Log LOG = LogFactory.getLog(BigQuerySchema.class);

    public TableInfo convertSchemaToTableInfo(String database, String table, HCatSchema hcatSchema) throws IOException {

        LOG.info("Incoming HCat table schema: " + hcatSchema.getSchemaAsTypeString());

        TableId tableId = TableId.of(database, table);

        Schema tableFields = Schema.of();

        StandardTableDefinition tableDefinition = StandardTableDefinition.of(tableFields);

        TableInfo tableInfo = TableInfo.of(tableId, tableDefinition);

        LOG.info("Converted BigQuery schema: " + tableInfo);

        return tableInfo;
    }

}
