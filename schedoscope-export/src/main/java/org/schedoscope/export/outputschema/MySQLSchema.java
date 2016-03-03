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

package org.schedoscope.export.outputschema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Contains the mysql specific mapping of the column types.
 *
 */
public class MySQLSchema extends AbstractSchema implements Schema {

    protected static final String JDBC_DRIVER_NAME = "com.mysql.jdbc.Driver";

    protected static final String JDBC_MYSQL_DEFAULT_STORAGE_ENGINE = "InnoDB";

    @SuppressWarnings("serial")
    private static final Map<String, String> columnTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("string", "text");
                    put("boolean", "boolean");
                    put("int", "int");
                    put("long", "bigint");
                    put("bigint", "bigint");
                    put("double", "double");
                    put("float", "float");
                    put("tinyint", "tinyint");
                }
            });

    @SuppressWarnings("serial")
    private static final Map<String, String> preparedStatementTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("text", "string");
                    put("boolean", "boolean");
                    put("int", "int");
                    put("bigint", "long");
                    put("double", "double");
                    put("float", "float");
                    put("tinyint", "int");
                }
            });

    /**
     * A class representing the MySQL dialect.
     * @param conf The Hadoop configuration object.
     */
    public MySQLSchema(Configuration conf) {
        super(conf);
        this.conf.set(Schema.JDBC_DRIVER_CLASS, JDBC_DRIVER_NAME);

    }

    @Override
    public Map<String, String> getColumnTypeMapping() {
        return columnTypeMapping;
    }

    @Override
    public Map<String, String> getPreparedStatementTypeMapping() {
        return preparedStatementTypeMapping;
    }

    @Override
    protected String getCreateTableSuffix() {
        return new String(" ENGINE=" + conf.get(JDBC_MYSQL_STORAGE_ENGINE, JDBC_MYSQL_DEFAULT_STORAGE_ENGINE)
                + " DEFAULT CHARSET=utf8");
    }
}
