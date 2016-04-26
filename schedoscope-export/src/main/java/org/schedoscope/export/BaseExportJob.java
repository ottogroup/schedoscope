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

package org.schedoscope.export;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.Tool;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;

/**
 * Base class with common functions to configure Job objects.
 */
public abstract class BaseExportJob extends Configured implements Tool {

	public static final String EXPORT_ANON_FIELDS = "export.anon.fields";

	public static final String EXPORT_ANON_SALT = "export.anon.salt";

	@Option(name = "-A", handler = StringArrayOptionHandler.class, usage = "a space separated list of fields to anonymize")
	protected String[] anonFields = new String[0];

	@Option(name = "-S", usage = "an optional salt used to anonymize fields")
	protected String exportSalt = "";

	protected Configuration getConfiguration() {

		if (getConf() == null)
			setConf(new Configuration());

		return getConf();
	}

	protected Configuration configureHiveMetaStore(Configuration conf, String metaStoreUris) {

		if (metaStoreUris.startsWith("thrift://")) {
			conf.set("hive.metastore.local", "false");
			conf.set(HiveConf.ConfVars.METASTOREURIS.varname, metaStoreUris);
		} else {
			conf.set("hive.metastore.local", "true");
			conf.unset(HiveConf.ConfVars.METASTOREURIS.varname);
			conf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, metaStoreUris);
		}
		return conf;
	}

	protected Configuration configureKerberos(Configuration conf, boolean isSecured, String principal) {

		if (isSecured) {
			conf.setBoolean(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, true);
			conf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, principal);

			if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
				conf.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
			}
		}
		return conf;
	}

	protected Configuration configureAnonFields(Configuration conf) {
		conf.setStrings(EXPORT_ANON_FIELDS, anonFields);
		conf.set(EXPORT_ANON_SALT, exportSalt);
		return conf;
	}
}
