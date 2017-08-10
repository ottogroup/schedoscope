package org.schedoscope.metascope.task.metastore;

import com.google.common.collect.Lists;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.task.metastore.model.MetastorePartition;
import org.schedoscope.metascope.task.metastore.model.MetastoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetastoreJdbcClient extends MetastoreClient {

    private static final Logger LOG = LoggerFactory.getLogger(MetastoreJdbcClient.class);

    private Connection connection;

    public MetastoreJdbcClient(MetascopeConfig config) {
        super(config);
    }

    @Override
    public void init() {
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception e) {
            LOG.error("com.mysql.jdbc.Driver", e);
        }

        try {
            this.connection = DriverManager.getConnection(config.getMetastoreJdbcUrl(), config.getMetastoreJdbcUser(),
              config.getMetastoreJdbcPassword());
        } catch (SQLException e) {
            LOG.error("Could not connect to hive metastore", e);
        }
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Could not close connection", e);
        }
    }

    @Override
    public MetastoreTable getTable(String databaseName, String tableName) {
        if (connection == null) {
            return null;
        }
        return null;
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, short size) {
        return Lists.newArrayList("root_jdbc");
    }

    @Override
    public List<List<String>> partitionLists(List<String> partitionNames, int size) {
        return Lists.partition(partitionNames, partitionNames.size());
    }

    @Override
    public List<MetastorePartition> listPartitions(String databaseName, String tableName, List<String> groupedPartitionNames) {
        if (connection == null) {
            return Lists.newArrayList();
        }

        List<MetastorePartition> partitions = new ArrayList<>();
        return partitions;
    }

}
