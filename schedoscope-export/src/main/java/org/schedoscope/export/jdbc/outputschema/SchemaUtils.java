/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.export.jdbc.outputschema;

import java.util.Locale;
import java.util.Map;

import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * This class provides utility functions to convert names and types between different database dialects.
 *
 */
public class SchemaUtils {

    /**
     * Converts the column names from HCatalogSchema to a given database dialect.
     *
     * @param inputSchema The HCatalog Schema containing the meta data.
     * @param schema The database schema dialect.
     * @return An array of string containing the names of the columns, order is important.
     */
    public static String[] getColumnNamesFromHcatSchema(HCatSchema inputSchema, Schema schema) {

        Map<String, String> columnNameMapping = schema.getColumnNameMapping();

        Object[] hcatInputNames = inputSchema.getFieldNames().toArray();
        String[] outputColumnNames = new String[hcatInputNames.length + 1];

        for (int i = 0; i < hcatInputNames.length; i++) {

            String fieldName = hcatInputNames[i].toString();

            if (columnNameMapping.containsKey(fieldName)) {
                fieldName = columnNameMapping.get(fieldName);
            }

            outputColumnNames[i] = fieldName;
        }

        outputColumnNames[hcatInputNames.length] = "used_filter";
        return outputColumnNames;
    }

    /**
     * Converts the column types from HCatSchema to the database dialect.
     *
     * @param inputSchema The HCatalog Schema containing the meta data.
     * @param schema The database schema dialect.
     * @return An array of string containing the column types, order is important.
     */
    public static String[] getColumnTypesFromHcatSchema(HCatSchema inputSchema, Schema schema) {

        Map<String, String> columnTypeMapping = schema.getColumnTypeMapping();

        String[] fieldTypes = new String[inputSchema.getFieldNames().size() + 1];

        for (int i = 0; i < inputSchema.getFieldNames().size(); i++) {

            String type = columnTypeMapping.get("string");

            if (!inputSchema.get(i).isComplex()) {

                if (inputSchema.get(i).getTypeString().toLowerCase(Locale.getDefault()) == null
                        || inputSchema.get(i).getTypeString().toLowerCase(Locale.getDefault()).equals("null")) {

                    type = columnTypeMapping.get("tinyint");
                } else {
                    type = columnTypeMapping.get(inputSchema.get(i).getTypeString().toLowerCase(Locale.getDefault()));
                }
            }
            fieldTypes[i] = type;
        }

        fieldTypes[inputSchema.getFieldNames().size()] = columnTypeMapping.get("string");
        return fieldTypes;
    }
}
