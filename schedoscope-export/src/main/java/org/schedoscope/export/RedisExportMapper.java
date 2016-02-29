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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisListWritable;
import org.schedoscope.export.outputformat.RedisOutputFormat;
import org.schedoscope.export.outputformat.RedisStringWritable;
import org.schedoscope.export.outputformat.RedisWritable;

public class RedisExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, RedisWritable> {


	private Configuration conf;

	private HCatSchema schema;

	private String keyName;

	private String valueName;

	private String keyPrefix;

	private Boolean append;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		RedisOutputFormat.checkKeyType(schema, conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME));
		RedisOutputFormat.checkValueType(schema, conf.get(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME));

		keyName = conf.get(RedisOutputFormat.REDIS_EXPORT_KEY_NAME);
		valueName = conf.get(RedisOutputFormat.REDIS_EXPORT_VALUE_NAME);

		keyPrefix = RedisOutputFormat.getExportKeyPrefix(conf);
		append = RedisOutputFormat.getAppend(conf);
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException  {

		Text redisKey = new Text(keyPrefix + value.getString(keyName, schema));
		RedisWritable redisValue = null;

		HCatFieldSchema fieldSchema = schema.get(valueName);

		switch(fieldSchema.getCategory()) {
		case MAP:
			redisValue = new RedisHashWritable(redisKey.toString(), (Map<String, String>) value.getMap(valueName, schema), append);
			break;
		case ARRAY:
			redisValue = new RedisListWritable(redisKey.toString(), (List<String>) value.getList(valueName, schema), append);
			break;
		case PRIMITIVE:
			redisValue = new RedisStringWritable(redisKey.toString(), value.getString(valueName, schema));
			break;
		case STRUCT:
			List<String> vals = (List<String>) value.getStruct(valueName, schema);
			HCatSchema structSchema = fieldSchema.getStructSubSchema();
			MapWritable structValue = new MapWritable();
			for (int i = 0; i < structSchema.size(); i++) {
				structValue.put(new Text(structSchema.get(i).getName()), new Text(vals.get(i)));
			}
			redisValue = new RedisHashWritable(redisKey, structValue, new BooleanWritable(false));
			break;
		}

		context.write(redisKey, redisValue);
	}
}
