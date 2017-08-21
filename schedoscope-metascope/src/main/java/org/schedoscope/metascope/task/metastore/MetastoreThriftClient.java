package org.schedoscope.metascope.task.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.task.metastore.model.MetastorePartition;
import org.schedoscope.metascope.task.metastore.model.MetastoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MetastoreThriftClient extends MetastoreClient {

    private static final Logger LOG = LoggerFactory.getLogger(MetastoreThriftClient.class);

    private HiveMetaStoreClient client;

    public MetastoreThriftClient(MetascopeConfig config) {
        super(config);
    }

    @Override
    public void init() {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.local", "false");
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, config.getMetastoreThriftUri());
        String principal = config.getKerberosPrincipal();
        if (principal != null && !principal.isEmpty()) {
            conf.setBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL, true);
            conf.setVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL, principal);
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
        }

        this.client = null;
        try {
            client = new HiveMetaStoreClient(conf);
        } catch (Exception e) {
            LOG.error("[MetastoreSyncTask] FAILED: Could not connect to hive metastore", e);
        }
    }

    @Override
    public MetastoreTable getTable(String databaseName, String tableName) {
        try {
            Table t = client.getTable(databaseName, tableName);
            return new MetastoreTable(t.getOwner(), t.getCreateTime() * 1000L, t.getSd().getInputFormat(),
              t.getSd().getOutputFormat(), t.getSd().getLocation(), t.getParameters().get(SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP));
        } catch (TException e) {
            LOG.error("Could not retrieve table from metastore", e);
            return null;
        }
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, short size) {
        try {
            return client.listPartitionNames(databaseName, tableName, size);
        } catch (TException e) {
            LOG.error("Could not retrieve partition names from metastore", e);
            return Lists.newArrayList();
        }
    }

    @Override
    public List<List<String>> partitionLists(List<String> partitionNames, int size) {
        return Lists.partition(partitionNames, size);
    }

    @Override
    public List<MetastorePartition> listPartitions(String databaseName, String tableName, List<String> groupedPartitionNames) {
        try {
            List<MetastorePartition> partitions = new ArrayList<>();
            List<Partition> partitionList = client.getPartitionsByNames(databaseName, tableName, groupedPartitionNames);
            for (Partition partition : partitionList) {
                MetastorePartition metastorePartition = new MetastorePartition();
                metastorePartition.setValues(partition.getValues());
                metastorePartition.setNumRows(partition.getParameters().get("numRows"));
                metastorePartition.setTotalSize(partition.getParameters().get("totalSize"));
                metastorePartition.setSchedoscopeTimestamp(partition.getParameters().get(SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP));
                partitions.add(metastorePartition);
            }
            return partitions;
        } catch (TException e) {
            LOG.error("Could not retrieve partitions from metastore", e);
            return Lists.newArrayList();
        }

    }

    @Override
    public void close() {
        this.client.close();
    }

}
