package org.schedoscope.export;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;

public class RedisExportMapper extends Mapper<WritableComparable<?>, HCatRecord, Text, NullWritable> {

	private static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	private static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	private Configuration conf;

	private HCatSchema schema;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		super.setup(context);
		conf = context.getConfiguration();
		schema = HCatInputFormat.getTableSchema(conf);

		HCatFieldSchema keyType = schema.get(REDIS_EXPORT_KEY_NAME);
		Category category = keyType.getTypeInfo().getCategory();

		if (category != Category.PRIMITIVE) {
			throw new IllegalArgumentException("key must be primitive type");
		}

		HCatFieldSchema valueType = schema.get(REDIS_EXPORT_VALUE_NAME);
		Category valueCat = valueType.getTypeInfo().getCategory();

		if ((valueCat != Category.PRIMITIVE) && (valueCat != Category.MAP) && (valueCat != Category.LIST)) {
			throw new IllegalArgumentException("value must be one of primitive, list or map type");
		}

	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value, Context context) {


	}
}
