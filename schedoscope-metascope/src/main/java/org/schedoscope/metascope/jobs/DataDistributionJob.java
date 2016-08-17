/**
 * Copyright 2015 Otto (GmbH & Co KG)
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
package org.schedoscope.metascope.jobs;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.DataDistributionEntity;
import org.schedoscope.metascope.model.FieldEntity;
import org.schedoscope.metascope.model.TableEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.repository.DataDistributionEntityRepository;
import org.schedoscope.metascope.repository.JobMetadataEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DataDistributionJob extends AsyncRepositoryJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataDistributionJob.class);

    public enum NumericType {
        INT, BIGINT, TINYINT, DOUBLE, FLOAT
    }

    public enum StringType {
        STRING
    }

    public enum BooleanType {
        BOOLEAN
    }

    public DataDistributionJob(TableEntity tableEntity, ViewEntity viewEntity, FieldEntity fieldEntity,
                               DataDistributionEntityRepository dataDistributionEntityRepository,
                               JobMetadataEntityRepository jobMetadataEntityRepository, MetascopeConfig config, String jobMetadataKey,
                               String jobMetadataField) {
        super(tableEntity, viewEntity, fieldEntity, dataDistributionEntityRepository, jobMetadataEntityRepository, config,
                jobMetadataKey, jobMetadataField);
    }

    @Override
    protected void execute() {
        LOG.info("DataDistributionJob for view '{}' and field '{}' started", viewEntity.getUrlPath(), fieldEntity.getName());
        if (isNumeric(fieldEntity.getType())) {
            numericDistribution();
        } else if (isString(fieldEntity.getType())) {
            stringDistribution();
        } else if (isBoolean(fieldEntity.getType())) {
            booleanDistribution();
        }
        LOG.info("DataDistributionJob for view '{}' and field '{}' finished", viewEntity.getUrlPath(),
                fieldEntity.getName());
    }

    @Transactional
    private void numericDistribution() {
        String fn = fieldEntity.getName();
        String tn = tableEntity.getFqdn();
        String sql = "select min(" + fn + ") as min, max(" + fn + ") as max, avg(" + fn + ") as avg, stddev_pop(" + fn
                + ") as stddev, sum(" + fn + ") as sum from " + tn;
        sql = addParameters(sql);

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                DataDistributionEntity dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "min", rs.getString("min"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "max", rs.getString("max"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "avg", rs.getString("avg"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "stddev", rs.getString("stddev"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "sum", rs.getString("sum"));
                dataDistributionEntityRepository.save(dde);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void stringDistribution() {
        String fn = fieldEntity.getName();
        String tn = tableEntity.getFqdn();
        String sql = "select min(" + fn + ") as min, max(" + fn + ") as max from " + tn;
        sql = addParameters(sql);

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                DataDistributionEntity dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "min", rs.getString("min"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "max", rs.getString("max"));
                dataDistributionEntityRepository.save(dde);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        sql = "select " + fn + " as enum, count(" + fn + ") as enumcount from " + tn;
        sql = addParameters(sql);
        sql += " group by " + fn;

        List<String> enumResults = new ArrayList<String>();
        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            int counter = 0;
            boolean isEnumeration = true;
            while (rs.next()) {
                enumResults.add(rs.getString("enum") + " (" + rs.getInt("enumcount") + ")");
                counter++;
                if (counter > 255) {
                    isEnumeration = false;
                    break;
                }
            }
            if (isEnumeration && enumResults.size() > 0) {
                String enumerationString = "";
                for (String enumeration : enumResults) {
                    if (!enumerationString.isEmpty()) {
                        enumerationString += "#";
                    }
                    enumerationString += enumeration;
                }
                DataDistributionEntity dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "enum", enumerationString);
                dataDistributionEntityRepository.save(dde);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void booleanDistribution() {
        String fn = fieldEntity.getName();
        String tn = tableEntity.getFqdn();
        String sql = "select sum(case when " + fn + " then 1 else 0 end) as true, " + "sum(case when not(" + fn
                + ") then 1 when isnull(" + fn + ") then 1 else 0 end) as false from " + tn;
        sql = addParameters(sql);

        try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                DataDistributionEntity dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "true",
                        rs.getString("true"));
                dataDistributionEntityRepository.save(dde);

                dde = new DataDistributionEntity(viewEntity.getUrlPath(), fn, "false", rs.getString("false"));
                dataDistributionEntityRepository.save(dde);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private boolean isNumeric(String type) {
        for (NumericType c : NumericType.values()) {
            if (c.name().equals(type)) {
                return true;
            }
        }
        return false;
    }

    private boolean isString(String type) {
        for (StringType c : StringType.values()) {
            if (c.name().equals(type)) {
                return true;
            }
        }
        return false;
    }

    private boolean isBoolean(String type) {
        for (BooleanType c : BooleanType.values()) {
            if (c.name().equals(type)) {
                return true;
            }
        }
        return false;
    }

}
