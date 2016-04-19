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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 * A utility class providing various checks for HCatSchemas and fields.
 */
public class HCatUtils {

	private static final String HASH_SALT = "vD75MqvaasIlCf7H";

	/**
	 * This function checks if the key type is a primitive type.
	 *
	 * @param schema
	 *            The HCatSchema.
	 * @param fieldName
	 *            The name of the field to check.
	 * @throws IOException
	 *             Is thrown in case of errors.
	 */
	public static void checkKeyType(HCatSchema schema, String fieldName)
			throws IOException {

		HCatFieldSchema keyType = schema.get(fieldName);
		HCatFieldSchema.Category category = keyType.getCategory();

		if (category != HCatFieldSchema.Category.PRIMITIVE) {
			throw new IllegalArgumentException("key must be primitive type");
		}
	}

	/**
	 * This function checks the type category of the value.
	 *
	 * @param schema
	 *            The HCatSchema.
	 * @param fieldName
	 *            The name of the field to check.
	 * @throws IOException
	 *             Is thrown in case of errors.
	 */
	public static void checkValueType(HCatSchema schema, String fieldName)
			throws IOException {

		HCatFieldSchema valueType = schema.get(fieldName);

		if (valueType.getCategory() == HCatFieldSchema.Category.MAP) {
			if (valueType.getMapValueSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException(
						"map value type must be a primitive type");
			}
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.ARRAY) {
			if (valueType.getArrayElementSchema().get(0).getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
				throw new IllegalArgumentException(
						"array element type must be a primitive type");
			}
		}

		if (valueType.getCategory() == HCatFieldSchema.Category.STRUCT) {
			HCatSchema structSchema = valueType.getStructSubSchema();
			for (HCatFieldSchema f : structSchema.getFields()) {
				if (f.getCategory() != HCatFieldSchema.Category.PRIMITIVE) {
					throw new IllegalArgumentException(
							"struct element type must be a primitive type");
				}
			}

		}
	}

	public static String getHashValueIfInList(String fieldName, String fieldValue, String[] anonFields) {

		if (ArrayUtils.contains(anonFields, fieldName)) {
			return DigestUtils.md5Hex(fieldValue + HASH_SALT);
		} else {
			return fieldValue;
		}
	}
}
