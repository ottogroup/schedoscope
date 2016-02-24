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

import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.schedoscope.export.outputformat.RedisHashWritable;
import org.schedoscope.export.outputformat.RedisListWritable;
import org.schedoscope.export.outputformat.RedisStringWritable;



public class RedisMRUtils {

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
		/**
		 * @TODO check if subtypes are primitive
		 */
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
}
