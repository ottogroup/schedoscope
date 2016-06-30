/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.tasks;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.tasks.repository.RepositoryDAO;
import org.schedoscope.metascope.util.SchedoscopeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetastoreSyncTask extends Task {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreSyncTask.class);

  private final static String SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP = "transformation.timestamp";

  private Configuration hadoopConfig;

  public MetastoreSyncTask(RepositoryDAO repo, DataSource dataSource, SolrFacade solr, MetascopeConfig config,
      SchedoscopeUtil schedoscopUtil) {
    super(repo, dataSource, solr, config, schedoscopUtil);
    this.hadoopConfig = new Configuration();
    this.hadoopConfig.set("fs.defaultFS", config.getHdfs());
  }

  @Override
  public boolean run(long start) {
    LOG.info("Sync repository with metastore");
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

    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(conf);
    } catch (Exception e) {
      LOG.info("[MetastoreSyncTask] FAILED: Could not connect to hive metastore", e);
      return false;
    }

    FileSystem fs;
    try {
      fs = FileSystem.get(hadoopConfig);
    } catch (IOException e) {
      LOG.info("[MetastoreSyncTask] FAILED: Could not connect to HDFS", e);
      return false;
    }

    Connection connection;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      LOG.info("[MetastoreSyncTask] FAILED: Could not connect to repository", e);
      return false;
    }

    LOG.info("Connected to metastore (" + config.getMetastoreThriftUri() + ")");
    List<TableEntity> allTables = repo.getTables(connection);
    List<ViewEntity> allViews = repo.getViews(connection);
    int tableSize = allTables.size();
    int counter = 1;
    for (TableEntity tableEntity : allTables) {
      Table mTable;
      try {
        mTable = client.getTable(tableEntity.getDatabaseName(), tableEntity.getTableName());
      } catch (Exception e) {
        LOG.warn("Could not retrieve table from metastore", e);
        continue;
      }
      LOG.info("[" + counter++ + "/" + tableSize + "] Get metastore information for table " + tableEntity.getFqdn());
      tableEntity.setTableOwner(mTable.getOwner());
      tableEntity.setCreatedAt(mTable.getCreateTime() * 1000L);
      tableEntity.setInputFormat(mTable.getSd().getInputFormat());
      tableEntity.setOutputFormat(mTable.getSd().getOutputFormat());
      tableEntity.setDataPath(mTable.getSd().getLocation());
      tableEntity.setDataSize(getDirectorySize(fs, tableEntity.getDataPath()));
      tableEntity.setPermissions(getPermission(fs, tableEntity.getDataPath()));
      List<ViewEntity> views = getViews(tableEntity.getFqdn(), allViews);
      if (views.size() == 1) {
        ViewEntity viewEntity = views.get(0);
        String schedoscopeTimestamp = mTable.getParameters().get(SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP);
        setTransformationTimestamp(schedoscopeTimestamp, tableEntity, viewEntity, mTable.getCreateTime() * 1000L,
            connection);
      } else {
        for (ViewEntity viewEntity : views) {
          Partition mPartition;
          try {
            mPartition = client.getPartition(tableEntity.getDatabaseName(), tableEntity.getTableName(),
                viewEntity.getParameterString());
          } catch (Exception e) {
            LOG.warn("Could not retrieve partition from metastore", e);
            continue;
          }
          if (mPartition != null) {
            String schedoscopeTimestamp = mPartition.getParameters().get(SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP);
            setTransformationTimestamp(schedoscopeTimestamp, tableEntity, viewEntity,
                mPartition.getCreateTime() * 1000L, connection);
          }
        }
      }
    }

    LOG.info("Updating tables ...");
    for (TableEntity tableEntity : allTables) {
      repo.insertOrUpdate(connection, tableEntity);
      solr.updateTablePartial(tableEntity, false);
    }

    try {
      connection.close();
    } catch (SQLException e) {
      LOG.error("Could not close connection to repository", e);
    }

    solr.commit();
    client.close();
    LOG.info("Sync with metastore finished");
    return true;
  }

  private void setTransformationTimestamp(String schedoscopeTimestamp, TableEntity tableEntity, ViewEntity viewEntity,
      Long createdAt, Connection connection) {
    try {
      if (schedoscopeTimestamp != null) {
        long newTransformationTimestamp = Long.parseLong(schedoscopeTimestamp);
        updateStatusAndTimestamps(tableEntity, viewEntity, newTransformationTimestamp, createdAt, connection);
      }
    } catch (Exception e) {
      LOG.warn("Could not parse timestamp", e);
    }
  }

  private void updateStatusAndTimestamps(TableEntity tableEntity, ViewEntity viewEntity,
      long newTransformationTimestamp, long createdAt, Connection connection) {
    if (viewEntity.getTransformationEnd() != newTransformationTimestamp) {
      viewEntity.setCreatedAt(createdAt);
      viewEntity.setTransformationEnd(newTransformationTimestamp);
      repo.updateViewStatus(connection, viewEntity, null, newTransformationTimestamp);
      solr.updateViewStatusInformation(viewEntity, newTransformationTimestamp, createdAt, false);
    }
    if (tableEntity.getLastTransformation() < newTransformationTimestamp) {
      tableEntity.setLastTransformation(newTransformationTimestamp);
    }
    if (tableEntity.getLastPartitionCreated() < createdAt) {
      tableEntity.setLastPartitionCreated(createdAt);
    }
  }

  private Long getDirectorySize(FileSystem fs, String path) {
    try {
      return fs.getContentSummary(new Path(path)).getSpaceConsumed();
    } catch (FileNotFoundException e) {
      LOG.warn("Directory '{}' does not exists", path);
      return 0L;
    } catch (IOException e) {
      LOG.error("Error retrieving size for directory '{}'", path, e);
      return 0L;
    }
  }

  private String getPermission(FileSystem fs, String path) {
    try {
      return fs.getFileStatus(new Path(path)).getPermission().toString();
    } catch (IllegalArgumentException | IOException e) {
      LOG.error("Error retrieving permissions for directory '{}'", path, e);
      return "-";
    }
  }

  private List<ViewEntity> getViews(String fqdn, List<ViewEntity> views) {
    List<ViewEntity> list = new ArrayList<ViewEntity>();
    for (ViewEntity viewEntity : views) {
      if (viewEntity.getFqdn().equals(fqdn)) {
        list.add(viewEntity);
      }
    }
    return list;
  }

}
