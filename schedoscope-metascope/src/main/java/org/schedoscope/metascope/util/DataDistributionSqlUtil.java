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

import org.schedoscope.metascope.model.MetascopeField;
import org.schedoscope.metascope.model.MetascopeTable;

import java.util.ArrayList;
import java.util.List;

public class DataDistributionSqlUtil {

  public enum NumericType {
    INT, BIGINT, TINYINT, DOUBLE, FLOAT
  }

  public enum StringType {
    STRING
  }

  public enum BooleanType {
    BOOLEAN
  }

  public enum CollectionType {
    MAP, LIST, ARRAY
  }

  public static String buildSql(MetascopeTable table) {
    String sql = new String();
    List<MetascopeField> fields = new ArrayList<>();

    for (MetascopeField metascopeField : table.getFields()) {
      if (isNumeric(metascopeField.getFieldType()) || isBoolean(metascopeField.getFieldType())
        || isString(metascopeField.getFieldType()) || isCollection(metascopeField.getFieldType())) {
        fields.add(metascopeField);
      }
    }

    for (MetascopeField field : fields) {
      if (sql.isEmpty()) {
        sql += "select count(*) as agg_rows, ";
      } else {
        sql += ",";
      }

      String fn = field.getFieldName();

      if (isNumeric(field.getFieldType())) {
        sql += "min(" + fn + ") as " + fn + "__min, max(" + fn + ") as " + fn + "__max, avg(" + fn + ") as " + fn + "__avg, stddev_pop(" + fn
          + ") as " + fn + "__stddev, sum(" + fn + ") as " + fn + "__sum";
      } else if (isBoolean(field.getFieldType())) {
        sql += "sum(case when " + fn + " then 1 else 0 end) as " + fn + "__true, " + "sum(case when not(" + fn
          + ") then 1 when isnull(" + fn + ") then 1 else 0 end) as " + fn + "__false";
      } else if (isString(field.getFieldType())) {
        sql += "min(" + fn + ") as " + fn + "__min, max(" + fn + ") as " + fn + "__max";
      } else if (isCollection(field.getFieldType())) {
        sql += "min(size(" + fn + ")) as " + fn + "__min, max(size(" + fn + ")) as " + fn + "__max";
      }
    }

    sql += " from " + table.getFqdn();

    return sql;
  }

  private static boolean isNumeric(String type) {
    for (NumericType c : NumericType.values()) {
      if (c.name().equalsIgnoreCase(type)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isString(String type) {
    for (StringType c : StringType.values()) {
      if (c.name().equalsIgnoreCase(type)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isBoolean(String type) {
    for (BooleanType c : BooleanType.values()) {
      if (c.name().equalsIgnoreCase(type)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isCollection(String type) {
    for (CollectionType c : CollectionType.values()) {
      if (type.toLowerCase().startsWith(c.name().toLowerCase())) {
        return true;
      }
    }
    return false;
  }

}
