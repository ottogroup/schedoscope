/**
 * Copyright 2016 Otto (GmbH & Co KG)
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

package org.schedoscope.export.jdbc.outputschema;

import org.apache.hadoop.conf.Configuration;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains the postgresql specific mapping of the column types.
 *
 */
public class PostgreSQLSchema extends AbstractSchema {

    protected static final String JDBC_DRIVER_NAME = "org.postgresql.Driver";

    @SuppressWarnings("serial")
    private static final Map<String, String> columnTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("string", "text");
                    put("boolean", "boolean");
                    put("int", "integer");
                    put("long", "bigint");
                    put("bigint", "bigint");
                    put("double", "double precision");
                    put("float", "real");
                    put("tinyint", "smallint");
                }
            });

    @SuppressWarnings("serial")
    private static final Map<String, String> preparedStatementTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("text", "string");
                    put("boolean", "boolean");
                    put("integer", "int");
                    put("bigint", "long");
                    put("double precision", "double");
                    put("real", "float");
                    put("smallint", "int");
                    put("json", "string");
                }
            });

    /**
     * The constructor to initialize the PostgreSQL dialect.
     *
     * @param conf
     *            The Hadoop configuration object.
     */
    public PostgreSQLSchema(Configuration conf) {

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
}
