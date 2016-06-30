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
package org.schedoscope.metascope.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class JDBCUtil {

  public static String getDatabaseColumnsForClass(Class<?> clazz) {
    String columns = "";
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (Modifier.isStatic(field.getModifiers())) {
        try {
          Object classField = field.get(null);
          if (classField instanceof String) {
            if (!columns.isEmpty()) {
              columns += ", ";
            }
            columns += classField;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return columns;
  }

  public static String getValuesCountForClass(Class<?> clazz) {
    String values = "";
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (Modifier.isStatic(field.getModifiers())) {
        try {
          Object classField = field.get(null);
          if (classField instanceof String) {
            if (!values.isEmpty()) {
              values += ", ";
            }
            values += "?";
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return values;
  }

  public static String getSetExpressionForClass(Class<?> clazz) {
    String setExpression = "";
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      field.setAccessible(true);
      if (Modifier.isStatic(field.getModifiers())) {
        try {
          Object classField = field.get(null);
          if (classField instanceof String) {
            if (!setExpression.isEmpty()) {
              setExpression += ", ";
            }
            setExpression += classField + "=?";
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return setExpression;
  }
}
