package org.schedoscope.metascope.service;

import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.model.MetascopeDataDistribution;
import org.schedoscope.metascope.model.MetascopeTable;
import org.schedoscope.metascope.repository.MetascopeDataDistributionRepository;
import org.schedoscope.metascope.util.DataDistributionSqlUtil;
import org.schedoscope.metascope.util.HiveServerConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MetascopeDataDistributionService {

    private static final Logger LOG = LoggerFactory.getLogger(MetascopeTableService.class);

    public enum Status {
        NotAvailable, Running, Finished
    }

    @Autowired
    private MetascopeDataDistributionRepository metascopeDataDistributionRepository;

    @Autowired
    private MetascopeConfig config;

    private Map<String, Boolean> runningJobs;

    public MetascopeDataDistributionService() {
        this.runningJobs = new ConcurrentHashMap<String, Boolean>();
    }

    public Status checkStatus(MetascopeTable table) {
        Boolean running = runningJobs.get(table.getFqdn());
        if (running != null && running == true) {
            return Status.Running;
        } else if (!metascopeDataDistributionRepository.findByFqdn(table.getFqdn()).isEmpty()) {
            return Status.Finished;
        } else {
            return Status.NotAvailable;
        }
    }

    public Map<String, MetascopeDataDistribution> getDataDistribution(MetascopeTable table) {
        Map<String, MetascopeDataDistribution> ddMap = new LinkedHashMap<>();
        List<MetascopeDataDistribution> ddList = metascopeDataDistributionRepository.findByFqdn(table.getFqdn());
        for (MetascopeDataDistribution distribution : ddList) {
            ddMap.put(distribution.getMetric(), distribution);
        }
        return ddMap;
    }

    @Async("background")
    public void calculateDistribution(MetascopeTable table) {
        runningJobs.put(table.getFqdn(), true);

        String sql = DataDistributionSqlUtil.buildSql(table);

        HiveServerConnection hiveConn = new HiveServerConnection(config);

        hiveConn.connect();

        if (hiveConn.getConnection() == null) {
            runningJobs.put(table.getFqdn(), false);
            return;
        }

        try {
            Statement stmt = hiveConn.getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            ResultSetMetaData rsmd = rs.getMetaData();

            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                columnNames.add(rsmd.getColumnName(i));
            }

            long ts = System.currentTimeMillis();
            if (rs.next()) {
                for (String columnName : columnNames) {
                    MetascopeDataDistribution mdd = new MetascopeDataDistribution();
                    mdd.setId(table.getFqdn() + "." + columnName);
                    mdd.setFqdn(table.getFqdn());
                    mdd.setMetric(columnName);
                    mdd.setValue(rs.getString(columnName));
                    metascopeDataDistributionRepository.save(mdd);
                }
            }
        } catch (SQLException e) {
            hiveConn.close();
            runningJobs.put(table.getFqdn(), false);
            LOG.error("Could not execute hive query", e);
        }

        hiveConn.close();

        runningJobs.put(table.getFqdn(), false);
    }

}
