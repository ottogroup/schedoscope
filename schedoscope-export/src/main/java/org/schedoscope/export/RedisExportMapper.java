package org.schedoscope.export;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisStringWritable;
import org.schedoscope.export.outputformat.RedisWritable;

public class RedisExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, RedisWritable> {

	public static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	public static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	private Configuration conf;

	private HCatSchema schema;

	private Class<?> RWKlass;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		HCatFieldSchema keyType = schema.get(conf.get(REDIS_EXPORT_KEY_NAME));
		HCatFieldSchema.Category category = keyType.getCategory();

		if (category != HCatFieldSchema.Category.PRIMITIVE) {
			throw new IllegalArgumentException("key must be primitive type");
		}

		HCatFieldSchema valueType = schema.get(conf.get(REDIS_EXPORT_VALUE_NAME));
		HCatFieldSchema.Category valueCat = valueType.getCategory();

		if ((valueCat != HCatFieldSchema.Category.PRIMITIVE) && (valueCat != HCatFieldSchema.Category.MAP) && (valueCat != HCatFieldSchema.Category.ARRAY)) {
			throw new IllegalArgumentException("value must be one of primitive, list or map type");
		}

		switch (valueCat) {
		case PRIMITIVE: RWKlass = RedisStringWritable.class;
		break;
		case MAP: RWKlass = RedisHashWritable.class;
		}
	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) throws IOException, InterruptedException  {

		Text redisKey = new Text(value.getString(conf.get(REDIS_EXPORT_KEY_NAME), schema));
		Map<?,?> valueMap = value.getMap(conf.get(REDIS_EXPORT_VALUE_NAME), schema);

		RedisWritable redisValue = null;
		try {
			Constructor<?> ctor = RWKlass.getConstructor(String.class, Map.class);
			redisValue = (RedisWritable) ctor.newInstance(redisKey.toString(), valueMap);

		} catch (Exception e) {

		}
		context.write(redisKey, redisValue);
	}
}
