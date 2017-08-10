package org.schedoscope.metascope.task.metastore;

import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.task.metastore.model.MetastorePartition;
import org.schedoscope.metascope.task.metastore.model.MetastoreTable;

import java.util.List;

public abstract class MetastoreClient {

    protected static final String SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP = "transformation.timestamp";

    protected MetascopeConfig config;

    public MetastoreClient(MetascopeConfig config) {
        this.config = config;
    }

    public abstract void init();
    public abstract void close();
    public abstract MetastoreTable getTable(String databaseName, String tableName);
    public abstract List<String> listPartitionNames(String databaseName, String tableName, short size);
    public abstract List<List<String>> partitionLists(List<String> partitionNames, int size);
    public abstract List<MetastorePartition> listPartitions(String databaseName, String tableName, List<String> groupedPartitionNames);

}
