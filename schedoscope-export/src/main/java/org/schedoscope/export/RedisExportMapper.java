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

	private Configuration conf;

	private HCatSchema schema;

	private Class<?> RWKlass;

	private Class<?> RVKlass;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		RedisMRUtils.checkKeyType(schema, conf.get(REDIS_EXPORT_KEY_NAME));
		RedisMRUtils.checkValueType(schema, conf.get(REDIS_EXPORT_VALUE_NAME));

		RWKlass = RedisMRUtils.getRedisWritableKlass(schema, conf.get(REDIS_EXPORT_VALUE_NAME));
		RVKlass = RedisMRUtils.getRedisValueKlass(schema, conf.get(REDIS_EXPORT_VALUE_NAME));

	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException  {

		Text redisKey = new Text(value.getString(conf.get(REDIS_EXPORT_KEY_NAME), schema));

		RedisWritable redisValue = null;
		try {
			Constructor<?> ctor = RWKlass.getConstructor(String.class, RVKlass);
			redisValue = (RedisWritable) ctor.newInstance(redisKey.toString(), RVKlass.cast(value.get(conf.get(REDIS_EXPORT_VALUE_NAME), schema)));

		} catch (Exception e) {
		}
		context.write(redisKey, redisValue);
	}
}
