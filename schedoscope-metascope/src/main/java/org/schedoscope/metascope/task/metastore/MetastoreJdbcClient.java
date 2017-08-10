package org.schedoscope.metascope.task.metastore;

import com.google.common.collect.Lists;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.task.metastore.model.MetastorePartition;
import org.schedoscope.metascope.task.metastore.model.MetastoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetastoreJdbcClient extends MetastoreClient {

    private static final Logger LOG = LoggerFactory.getLogger(MetastoreJdbcClient.class);

    private Connection connection;

    private Map<Long, String> tableIdToTableName;
    private Map<Long, String> databaseIdToDatabaseName;
    private Map<String, Long> tableNameToTableId;
    private Map<String, Long> databaseNameToDatabaseId;

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
            LOG.error("Could not connect to hive metastore via JDBC", e);
        }

        this.databaseIdToDatabaseName = new HashMap<>();
        this.databaseNameToDatabaseId = new HashMap<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select DB_ID, NAME from DBS");
            while (rs.next()) {
                long db_id = rs.getLong("DB_ID");
                String name = rs.getString("NAME");
                this.databaseIdToDatabaseName.put(db_id, name);
                this.databaseNameToDatabaseId.put(name, db_id);
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve database information", e);
        }

        this.tableIdToTableName = new HashMap<>();
        this.tableNameToTableId = new HashMap<>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select TBL_ID, DB_ID, TBL_NAME from TBLS");
            while (rs.next()) {
                long tbl_id = rs.getLong("TBL_ID");
                long db_id = rs.getLong("DB_ID");
                String name = rs.getString("TBL_NAME");
                String dbName = databaseIdToDatabaseName.get(db_id);
                this.tableIdToTableName.put(tbl_id, dbName + "." + name);
                this.tableNameToTableId.put(dbName + "." + name, tbl_id);
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table information", e);
        }
        System.out.println();
    }

    @Override
    public MetastoreTable getTable(String databaseName, String tableName) {
        if (connection == null) {
            return null;
        }

        try {
            Statement stmt = connection.createStatement();
            String query = new StringBuilder()
                    .append("select OWNER, CREATE_TIME, INPUT_FORMAT, OUTPUT_FORMAT, LOCATION, PARAM_VALUE ")
                    .append("from (")
                    .append("  select TBL_ID, SD_ID, OWNER, CREATE_TIME from TBLS where TBL_NAME=\"" + tableName + "\" and DB_ID=" + databaseNameToDatabaseId.get(databaseName))
                    .append(") t ")
                    .append("join SDS sd on t.SD_ID = sd.SD_ID ")
                    .append("left join (")
                    .append("select * from TABLE_PARAMS where TBL_ID=3 and PARAM_KEY=\"" + SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP + "\"")
                    .append(") tp on t.TBL_ID = tp.TBL_ID")
                    .toString();
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return new MetastoreTable(rs.getString("OWNER"), rs.getInt("CREATE_TIME") * 1000L, rs.getString("INPUT_FORMAT"),
                        rs.getString("OUTPUT_FORMAT"), rs.getString("LOCATION"), rs.getString("PARAM_VALUE"));
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table from metastore", e);
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
        try {
            Statement stmt = connection.createStatement();
            String query = new StringBuilder()
                    .append("select PART_NAME, NUM_ROWS, TOTAL_SIZE, TIMESTAMP from ( ")
                    .append("  select TBL_ID, PART_ID, PART_NAME from PARTITIONS where TBL_ID=" + tableNameToTableId.get(databaseName + "." + tableName))
                    .append(") p join ( ")
                    .append("  select PART_ID, PARAM_VALUE as NUM_ROWS from PARTITION_PARAMS where PARAM_KEY=\"numRows\" ")
                    .append(") nr on p.PART_ID = nr.PART_ID join (")
                    .append("  select PART_ID, PARAM_VALUE as TOTAL_SIZE from PARTITION_PARAMS where PARAM_KEY=\"totalSize\" ")
                    .append(") ts on p.PART_ID = ts.PART_ID join (")
                    .append("  select PART_ID, PARAM_VALUE as TIMESTAMP from PARTITION_PARAMS where PARAM_KEY=\"transformation.timestamp\" ")
                    .append(") tt on p.PART_ID = tt.PART_ID")
                    .toString();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                MetastorePartition metastorePartition = new MetastorePartition();
                String partitionName = rs.getString("PART_NAME");
                String num_rows = rs.getString("NUM_ROWS");
                String total_size = rs.getString("TOTAL_SIZE");
                String timestamp = rs.getString("TIMESTAMP");
                metastorePartition.setValuesFromName("/" + partitionName);
                metastorePartition.setNumRows(num_rows);
                metastorePartition.setTotalSize(total_size);
                metastorePartition.setSchedoscopeTimestamp(timestamp);
                partitions.add(metastorePartition);
            }
        } catch (SQLException e) {
            LOG.error("Could not retrieve table from metastore", e);
        }
        return partitions;
    }

    @Override
    public void close() {
        this.tableIdToTableName = null;
        this.databaseIdToDatabaseName = null;
        this.tableNameToTableId = null;
        this.databaseNameToDatabaseId = null;

        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Could not close connection", e);
            connection = null;
        }
    }

}
