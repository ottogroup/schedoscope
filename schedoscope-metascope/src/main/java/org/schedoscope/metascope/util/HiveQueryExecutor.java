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
package org.schedoscope.metascope.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.schedoscope.metascope.config.MetascopeConfig;
import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.util.model.HiveQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Component
public class HiveQueryExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryExecutor.class);

  @Autowired
  private MetascopeConfig config;

  @PostConstruct
  private void init() {
    Configuration conf = new Configuration();
    String principal = config.getKerberosPrincipal();
    if (principal != null && !principal.isEmpty()) {
      conf.set("hive.metastore.sasl.enabled", "true");
      conf.set("hive.metastore.kerberos.principal", principal);
      conf.set("hadoop.security.authentication", "kerberos");
      UserGroupInformation.setConfiguration(conf);
    }
    try {
      Class.forName(config.getHiveJdbcDriver());
    } catch (ClassNotFoundException e) {
      LOG.error("Hive JDBC driver not found", e);
    }
  }

  @Transactional
  public HiveQueryResult executeQuery(String databaseName, String tableName, String fields, Set<MetascopeField> parameters,
                                      Map<String, String> params) {
    List<List<String>> rows = new ArrayList<List<String>>();

    HiveServerConnection hiveConn = new HiveServerConnection(config);

    hiveConn.connect();

    if (hiveConn.getConnection() == null) {
      return new HiveQueryResult("Could not connect to HiveServer2");
    }

    String where = "";
    List<String> values = new ArrayList<String>();
    if (params != null) {
      for (Entry<String, String> param : params.entrySet()) {
        if (param.getKey().equals("fqdn") || param.getKey().equals("_csrf")) {
          continue;
        }
        if (!param.getValue().isEmpty()) {
          boolean parameterExists = false;
          for (MetascopeField parameter : parameters) {
            if (parameter.getFieldName().equals(param.getKey())) {
              parameterExists = true;
            }
          }
          if (!parameterExists) {
            hiveConn.close();
            return new HiveQueryResult("Query not allowed");
          }

          if (!where.isEmpty()) {
            where += " AND ";
          }
          where += param.getKey() + "=?";
          values.add(param.getValue());
        }
      }
    }

    String sql = " SELECT " + fields;
    String parameterList = "";
    for (MetascopeField parameter : parameters) {
      sql += "," + parameter.getFieldName();
    }
    sql += parameterList;
    sql += " FROM " + databaseName + "." + tableName;
    sql += where.isEmpty() ? "" : " WHERE " + where;
    sql += " LIMIT 10";

    List<String> header = new ArrayList<String>();
    try {
      PreparedStatement pstmt = hiveConn.getConnection().prepareStatement(sql);
      for (int i = 1; i <= values.size(); i++) {
        pstmt.setString(i, values.get(i - 1));
      }
      ResultSet rs = pstmt.executeQuery();
      ResultSetMetaData rsmd = rs.getMetaData();

      for (int i = 1; i <= rsmd.getColumnCount(); i++) {
        header.add(rsmd.getColumnName(i));
      }

      while (rs.next()) {
        List<String> row = new ArrayList<String>();
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
          Object val = rs.getObject(i);
          String strVal = (val == null ? null : val.toString());
          row.add(strVal);
        }
        rows.add(row);
      }
    } catch (SQLException e) {
      LOG.error("Could not execute query", e);
      hiveConn.close();
      return new HiveQueryResult(e.getMessage());
    }

    hiveConn.close();
    return new HiveQueryResult(header, rows);
  }

}
