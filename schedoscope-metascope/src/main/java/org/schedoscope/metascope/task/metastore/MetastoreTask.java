/**
 * Copyright 2017 Otto (GmbH & Co KG)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope.metascope.task.metastore;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.index.SolrFacade;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.model.MetascopeView;
import org.schedoscope.metascope.repository.jdbc.RawJDBCSqlRepository;
import org.schedoscope.metascope.task.Task;
import org.schedoscope.metascope.task.metastore.model.MetastorePartition;
import org.schedoscope.metascope.task.metastore.model.MetastoreTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Component
public class MetastoreTask extends Task {

    private static final Logger LOG = LoggerFactory.getLogger(MetastoreTask.class);

    @Autowired
    private MetascopeConfig config;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private SolrFacade solrFacade;

    private MetastoreClient metastoreClient;

    public MetastoreTask(MetastoreClient metastoreClient) {
        this.metastoreClient = metastoreClient;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean run(RawJDBCSqlRepository sqlRepository, long start) {
        LOG.info("Sync repository with metastore");

        metastoreClient.init();

        FileSystem fs;
        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.set("fs.defaultFS", config.getHdfs());
            fs = FileSystem.get(hadoopConfig);
        } catch (IOException e) {
            LOG.info("[MetastoreSyncTask] FAILED: Could not connect to HDFS", e);
            metastoreClient.close();
            return false;
        }

        Connection connection;
        try {
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            LOG.error("Could not retrieve database connection.", e);
            return false;
        }

        LOG.info("Connected to metastore (" + config.getMetastoreThriftUri() + ")");

        List<MetascopeTable> allTables = sqlRepository.findAllTables(connection);

        for (MetascopeTable table : allTables) {
            LOG.info("Get metastore information for table " + table.getFqdn());

            try {
                MetastoreTable mTable = metastoreClient.getTable(table.getDatabaseName(), table.getTableName());

                if (mTable == null) {
                    LOG.error("Could not retrieve table from metastore.");
                    continue;
                }

                table.setTableOwner(mTable.getOwner());
                table.setCreatedAt(mTable.getCreateTime() * 1000L);
                table.setInputFormat(mTable.getInputFormat());
                table.setOutputFormat(mTable.getOutputFormat());
                table.setDataPath(mTable.getLocation());
                try {
                    table.setDataSize(getDirectorySize(fs, table.getDataPath()));
                    table.setPermissions(getPermission(fs, table.getDataPath()));
                } catch (IllegalArgumentException e) {
                    LOG.warn("Could not retrieve dir size: " + e.getMessage());
                    LOG.debug("ERROR: Could not read HDFS metadata", e);
                }

                long maxLastTransformation = -1;

                List<String> partitionNames = metastoreClient.listPartitionNames(table.getDatabaseName(), table.getTableName(), (short) -1);

                List<MetascopeView> views = sqlRepository.findViews(connection, table.getFqdn());
                List<List<String>> groupedPartitions = metastoreClient.partitionLists(partitionNames, 10000);
                for (List<String> groupedPartitionNames : groupedPartitions) {
                    List<MetastorePartition> partitions = metastoreClient.listPartitions(table.getDatabaseName(), table.getTableName(), groupedPartitionNames);
                    List<MetascopeView> changedViews = new ArrayList<>();
                    for (MetastorePartition partition : partitions) {
                        MetascopeView view = getView(views, partition);
                        if (view == null) {
                            //a view which is not registered as a partition in hive metastore should not exists ...
                            continue;
                        }
                        
                        view.setTable(table);

                        String numRows = partition.getNumRows();
                        if (numRows != null) {
                            view.setNumRows(Long.parseLong(numRows));
                        }
                        String totalSize = partition.getTotalSize();
                        if (totalSize != null) {
                            view.setTotalSize(Long.parseLong(totalSize));
                        }
                        String lastTransformation = partition.getSchedoscopeTimestamp();
                        if (lastTransformation != null) {
                            long ts = Long.parseLong(lastTransformation);
                            view.setLastTransformation(ts);
                            if (ts > maxLastTransformation) {
                                maxLastTransformation = ts;
                            }
                        }
                        solrFacade.updateViewEntity(view, false);
                        changedViews.add(view);
                    }
                    sqlRepository.insertOrUpdateViewMetadata(connection, changedViews);
                    solrFacade.commit();
                }

                if (maxLastTransformation != -1) {
                    table.setLastTransformation(maxLastTransformation);
                } else {
                    String ts = mTable.getSchedoscopeTimestamp();//mTable.getParameters().get(SCHEDOSCOPE_TRANSFORMATION_TIMESTAMP);
                    if (ts != null) {
                        long lastTransformationTs = Long.parseLong(ts);
                        table.setLastTransformation(lastTransformationTs);
                        MetascopeView rootView = views.get(0);
                        rootView.setTable(table);
                        rootView.setLastTransformation(lastTransformationTs);
                        solrFacade.updateViewEntity(rootView, false);
                    }
                }

                sqlRepository.saveTable(connection, table);
                solrFacade.updateTableMetastoreData(table, true);
            } catch (Exception e) {
                LOG.warn("Could not retrieve table from metastore", e);
                continue;
            }

        }

        /* commit to index */
        solrFacade.commit();

        metastoreClient.close();

        try {
            fs.close();
        } catch (IOException e) {
            LOG.warn("Could not close connection to HDFS", e);
        }

        try {
            connection.close();
        } catch (SQLException e) {
            LOG.error("Could not close connection", e);
        }

        LOG.info("Sync with metastore finished");
        return true;
    }

    private MetascopeView getView(List<MetascopeView> views, MetastorePartition partition) {
        for (MetascopeView view : views) {
            if (view.getParameterValues().equals(partition.getValues())) {
                return view;
            }
        }
        return null;
    }

    private Long getDirectorySize(FileSystem fs, String path) {
        try {
            return fs.getContentSummary(new Path(path)).getSpaceConsumed();
        } catch (FileNotFoundException e) {
            LOG.warn("Directory '{}' does not exists", path);
            return 0L;
        } catch (IOException e) {
            LOG.error(
                    "Error retrieving size for directory '{}'", path, e);
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

}
