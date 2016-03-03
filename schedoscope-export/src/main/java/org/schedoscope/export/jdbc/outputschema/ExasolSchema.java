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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * Contains the exasol specific mapping of the column types.
 *
 */
public class ExasolSchema extends AbstractSchema implements Schema {

    protected static final String JDBC_DRIVER_NAME = "com.exasol.jdbc.EXADriver";

    @SuppressWarnings("serial")
    private static final Map<String, String> columnTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("string", "varchar(100000)");
                    put("boolean", "boolean");
                    put("int", "int");
                    put("long", "bigint");
                    put("bigint", "bigint");
                    put("double", "double");
                    put("float", "float");
                    put("tinyint", "int");
                }
            });

    @SuppressWarnings("serial")
    private static final Map<String, String> preparedStatementTypeMapping = Collections
            .unmodifiableMap(new HashMap<String, String>() {
                {
                    put("varchar(100000)", "string");
                    put("boolean", "boolean");
                    put("int", "int");
                    put("bigint", "long");
                    put("double", "double");
                    put("float", "float");
                    put("tinyint", "int");
                }
            });

    /**
     * The constructor to initialize the
     * Exasol dialect.
     *
     * @param conf The Hadoop configuration object.
     */
    public ExasolSchema(Configuration conf) {
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
    protected String getDistributedByClause() {
        if (conf.get(JDBC_EXASOL_DISTRIBUTED_CLAUSE) != null) {
            return new String(", DISTRIBUTE BY " + conf.get(JDBC_EXASOL_DISTRIBUTED_CLAUSE + "\n"));
        } else {
            return "";
        }
    }
}
