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
package org.schedoscope.export.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisListWritable;
import org.schedoscope.export.outputformat.RedisStringWritable;

public class RedisMRUtils {

	public static final String REDIS_EXPORT_KEY_NAME = "redis.export.key.name";

	public static final String REDIS_EXPORT_VALUE_NAME = "redis.export.value.name";

	public static final String REDIS_EXPORT_KEY_PREFIX = "redis.export.key.prefix";

	public static void checkKeyType(HCatSchema schema, String fieldName) throws IOException {

		HCatFieldSchema keyType = schema.get(fieldName);
		HCatFieldSchema.Category category = keyType.getCategory();

		if (category != HCatFieldSchema.Category.PRIMITIVE) {
			throw new IllegalArgumentException("key must be primitive type");
		}
	}

	public static void checkValueType(HCatSchema schema, String fieldName) throws IOException {

		HCatFieldSchema valueType = schema.get(fieldName);
		HCatFieldSchema.Category valueCat = valueType.getCategory();

		if ((valueCat != HCatFieldSchema.Category.PRIMITIVE) && (valueCat != HCatFieldSchema.Category.MAP) && (valueCat != HCatFieldSchema.Category.ARRAY)) {
			throw new IllegalArgumentException("value must be one of primitive, list or map type");
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.MAP) {
			if (valueType.getMapValueSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException("map value type must be a primitive type");
			}
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.ARRAY) {
			if (valueType.getArrayElementSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException("array element type must be a primitive type");
			}
		}
	}

	public static Class<?> getRedisValueKlass(HCatSchema schema, String fieldName) throws IOException {
		HCatFieldSchema.Category category = schema.get(fieldName).getCategory();

		Class<?> RVKlass = null;

		switch (category) {
		case PRIMITIVE:
			RVKlass = String.class;
			break;
		case MAP:
			RVKlass = Map.class;
			break;
		case ARRAY:
			RVKlass = List.class;
			break;
		case STRUCT:
			break;
		default:
			break;
		}
		return RVKlass;
	}

	public static Class<?> getRedisWritableKlass(HCatSchema schema, String fieldName) throws IOException {
		HCatFieldSchema.Category category = schema.get(fieldName).getCategory();

		Class<?> RWKlass = null;

		switch (category) {
		case PRIMITIVE:
			RWKlass = RedisStringWritable.class;
			break;
		case MAP:
			RWKlass = RedisHashWritable.class;
			break;
		case ARRAY:
			RWKlass = RedisListWritable.class;
			break;
		case STRUCT:
			break;
		default:
			break;
		}
		return RWKlass;
	}

	public static String getExportKeyPrefix(Configuration conf) {

		String prefix = conf.get(RedisMRUtils.REDIS_EXPORT_KEY_PREFIX, "");
		StringBuilder keyPrefixBuilder = new StringBuilder();
		if (!prefix.isEmpty()) {
			keyPrefixBuilder.append(prefix).append("_");
		}
		return keyPrefixBuilder.toString();
	}
}
