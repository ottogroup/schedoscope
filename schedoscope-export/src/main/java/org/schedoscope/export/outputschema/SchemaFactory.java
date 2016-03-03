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

import org.apache.hadoop.conf.Configuration;

/**
 * The SchemaFactory returns a concrete Schema implementation determined by the given JDBC connection string.
 */
public class SchemaFactory {

    /**
     * Returns a concrete schema implementation determined by the JDBC connection string.
     *
     * @param dbConnectionString The JDBC connection string.
     * @param conf The Hadoop configuration object.
     * @return A concrete {@link Schema} implementation.
     */
    public static Schema getSchema(String dbConnectionString, Configuration conf) {
        String dialect = getDialect(dbConnectionString);
        if (dialect.equals("exa")) {
            return new ExasolSchema(conf);
        } else if (dialect.equals("derby")) {
            return new DerbySchema(conf);
        } else if (dialect.equals("mysql")) {
            return new MySQLSchema(conf);
        } else if (dialect.equals("postgresql")) {
            return new PostgreSQLSchema(conf);
        } else {
            throw new IllegalArgumentException(dbConnectionString + " not a valid jdbc connection string");
        }
    }

    /**
     * Returns a concrete schema implementation determined by the JDBC connection string, extracted from the Hadoop
     * configuration object.
     *
     * @param conf The Hadoop configuration object.
     * @return A concrete {@link Schema} implementation.
     */
    public static Schema getSchema(Configuration conf) {
        String dbConnectionString = conf.get(Schema.JDBC_CONNECTION_STRING);
        return getSchema(dbConnectionString, conf);
    }

    private static String getDialect(String dbConnectionString) {
        String[] parts = dbConnectionString.split(":");
        if (parts.length >= 2 && !parts[1].equals("")) {
            return parts[1];
        }
        throw new IllegalArgumentException(dbConnectionString + " not a valid jdbc connection string");
    }
}
