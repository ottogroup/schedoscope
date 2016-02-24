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
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.outputformat.RedisWritable;
import org.schedoscope.export.utils.RedisMRUtils;

public class RedisExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, RedisWritable> {

	public static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	public static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	public static final String REDIS_EXPORT_KEY_PREFIX = "redis.export.key.prefix";

	private Configuration conf;

	private HCatSchema schema;

	private Class<?> RWKlass;

	private Class<?> RVKlass;

	private String keyPrefix;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		RedisMRUtils.checkKeyType(schema, conf.get(REDIS_EXPORT_KEY_NAME));
		RedisMRUtils.checkValueType(schema, conf.get(REDIS_EXPORT_VALUE_NAME));

		RWKlass = RedisMRUtils.getRedisWritableKlass(schema, conf.get(REDIS_EXPORT_VALUE_NAME));
		RVKlass = RedisMRUtils.getRedisValueKlass(schema, conf.get(REDIS_EXPORT_VALUE_NAME));


		String prefix = conf.get(REDIS_EXPORT_KEY_PREFIX, "");
		StringBuilder builderKey = new StringBuilder();
		if (!prefix.isEmpty()) {
			builderKey.append(prefix).append("_");
		}
		keyPrefix = builderKey.toString();
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException  {

		Text redisKey = new Text(keyPrefix + value.getString(conf.get(REDIS_EXPORT_KEY_NAME), schema));
		RedisWritable redisValue = null;
		try {
			Constructor<?> ctor = RWKlass.getConstructor(String.class, RVKlass);
			redisValue = (RedisWritable) ctor.newInstance(redisKey.toString(), RVKlass.cast(value.get(conf.get(REDIS_EXPORT_VALUE_NAME), schema)));

		} catch (Exception e) {
		}
		context.write(redisKey, redisValue);
	}
}
