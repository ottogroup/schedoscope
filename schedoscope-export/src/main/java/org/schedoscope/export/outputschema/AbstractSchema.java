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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

/**
 * This class provides a couple of functions
 * common to all concrete Schema implementations.
 *
 * @author Otto
 *
 */
abstract public class AbstractSchema implements Schema {

	protected Configuration conf;

	@Override
	public String getTable() {
		return conf.get(Schema.JDBC_OUTPUT_TABLE);
	}

	@Override
	public String[] getColumnNames() {
		return conf.getStrings(Schema.JDBC_OUTPUT_COLUMN_NAMES);
	}

	@Override
	public String[] getColumnTypes() {
		return conf.getStrings(Schema.JDBC_OUTPUT_COLUMN_TYPES);
	}

	@Override
	public String getCreateTableQuery() {
		return conf.get(Schema.JDBC_CREATE_TABLE_QUERY);
	}

	@Override
	public int getNumberOfPartitions() {
		return conf.getInt(Schema.JDBC_NUMBER_OF_PARTITIONS, 1);
	}

	@Override
	public int getCommitSize() {
		return conf.getInt(Schema.JDBC_COMMIT_SIZE, 1);
	}

	@Override
	public String getFilter() {
		return conf.get(Schema.JDBC_INPUT_FILTER);
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public Map<String, String> getColumnNameMapping() {
		Map<String, String> columnNames = new HashMap<String, String>();
		columnNames.put("year", "data_year");
		columnNames.put("month", "data_month");
		columnNames.put("day", "data_day");
		return columnNames;
	}
}
