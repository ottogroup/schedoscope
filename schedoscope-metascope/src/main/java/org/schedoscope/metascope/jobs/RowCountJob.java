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
package org.schedoscope.metascope.jobs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.schedoscope.metascope.conf.MetascopeConfig;
import org.schedoscope.metascope.model.ParameterValueEntity;
import org.schedoscope.metascope.model.ViewEntity;
import org.schedoscope.metascope.repository.JobMetadataEntityRepository;
import org.schedoscope.metascope.repository.ViewEntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

public class RowCountJob extends AsyncRepositoryJob {

  private static final Logger LOG = LoggerFactory.getLogger(RowCountJob.class);

  public RowCountJob(ViewEntity viewEntity, ViewEntityRepository viewEntityRepository,
      JobMetadataEntityRepository jobMetadataEntityRepository, MetascopeConfig config, String jobMetadataKey,
      String jobMetadataField) {
    super(viewEntity, viewEntityRepository, jobMetadataEntityRepository, config, jobMetadataKey, jobMetadataField);
  }

  @Override
  @Transactional
  protected void execute() {
    LOG.info("RowCountJob for view '{}' started", viewEntity.getUrlPath());
    /* get count for partition */
    long rows = getRows(viewEntity);

    /* save view entity */
    viewEntity = viewEntityRepository.findByUrlPath(viewEntity.getUrlPath());
    viewEntity.setRows(rows);
    viewEntity.setRowJobFinished(true);
    viewEntityRepository.save(viewEntity);
    LOG.info("RowCountJob for view '{}' finished", viewEntity.getUrlPath());
  }

  private long getRows(ViewEntity viewEntity) {
    try {
      String sql = "select count(*) from " + viewEntity.getFqdn();
      if (!viewEntity.getParameters().isEmpty()) {
        sql += " where ";
        String whereCond = "";
        for (ParameterValueEntity kve : viewEntity.getParameters()) {
          if (!whereCond.isEmpty()) {
            whereCond += " and ";
          }
          whereCond += kve.getKey() + " = '" + kve.getValue() + "'";
        }
        sql += whereCond;
      }

      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        return rs.getLong(1);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return 0;
  }

}
