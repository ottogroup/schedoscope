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


	private Configuration conf;

	private HCatSchema schema;

	private Class<?> RWClazz;

	private Class<?> RVClazz;

	private String keyPrefix;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		RedisMRUtils.checkKeyType(schema, conf.get(RedisMRUtils.REDIS_EXPORT_KEY_NAME));
		RedisMRUtils.checkValueType(schema, conf.get(RedisMRUtils.REDIS_EXPORT_VALUE_NAME));

		RWClazz = RedisMRUtils.getRedisWritableClazz(schema, conf.get(RedisMRUtils.REDIS_EXPORT_VALUE_NAME));
		RVClazz = RedisMRUtils.getRedisValueClazz(schema, conf.get(RedisMRUtils.REDIS_EXPORT_VALUE_NAME));

		keyPrefix = RedisMRUtils.getExportKeyPrefix(conf);
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException  {

		Text redisKey = new Text(keyPrefix + value.getString(conf.get(RedisMRUtils.REDIS_EXPORT_KEY_NAME), schema));
		RedisWritable redisValue = null;
		try {
			Constructor<?> ctor = RWClazz.getConstructor(String.class, RVClazz);
			redisValue = (RedisWritable) ctor.newInstance(redisKey.toString(), RVClazz.cast(value.get(conf.get(RedisMRUtils.REDIS_EXPORT_VALUE_NAME), schema)));

		} catch (Exception e) {
		}
		context.write(redisKey, redisValue);
	}
}
