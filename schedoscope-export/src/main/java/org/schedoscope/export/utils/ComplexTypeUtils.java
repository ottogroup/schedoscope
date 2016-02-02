package org.schedoscope.export.utils;

import java.util.List;
import java.util.Map;

import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Category;

public class ComplexTypeUtils {

	private static String removeJsonFormatCharacters(String value) {
		value = value.replace("\n", " ");
		value = value.replace("\r", " ");
		value = value.replace("\"", "");
		value = value.replace("\\\\", "");
		value = value.replace(":", "");
		value = value.replace(",", "");
		value = value.replace("{", "");
		value = value.replace("}", "");
		value = value.replace("\t", " ");

		return value;

	}

	public static String structToJSONString(HCatRecord record, int pos,
			HCatSchema schema) throws InterruptedException {

		StringBuilder jsonString = new StringBuilder();

		try {
			jsonString.append("{");
			for (int sf = 0; sf < schema.get(pos).getStructSubSchema().size(); sf++) {
				String stuctKey = schema.get(pos).getStructSubSchema().get(sf)
						.getName();

				jsonString.append("\"");
				jsonString.append(stuctKey);

				if (record.getStruct(schema.get(pos).getName(), schema).get(sf) != null) {
					String structVal = removeJsonFormatCharacters(record
							.getStruct(schema.get(pos).getName(), schema)
							.get(sf).toString());
					jsonString.append("\":\"");
					jsonString.append(structVal);
					jsonString.append("\"");
				} else {
					jsonString.append("\":");
					jsonString.append("null");
				}

				if (sf != schema.get(pos).getStructSubSchema().size() - 1) {
					jsonString.append(",");
				}
			}
			return jsonString.append("}").toString();
		} catch (HCatException e) {
			throw new InterruptedException(e.getMessage());
		}

	}

	public static String arrayToJSONString(HCatRecord record, int pos,
			HCatSchema schema) throws InterruptedException {

		StringBuilder jsonString = new StringBuilder();

		try {
			jsonString.append("[");
			HCatFieldSchema entrySchema = schema.get(pos)
					.getArrayElementSchema().get(0);
			List<?> entries = record.getList(schema.get(pos).getName(), schema);

			for (int i = 0; i < entries.size(); i++) {
				Object entry = entries.get(i);

				if (entry != null && !entrySchema.isComplex()) {
					String value = removeJsonFormatCharacters(entry.toString());
					if (!value.trim().equals("")) {
						jsonString.append("\"");
						jsonString.append(value);
						jsonString.append("\"");

					}

				} else if (entry != null && entrySchema.isComplex()) {
					if (entrySchema.getCategory() == Category.STRUCT) {
						String nestedValue = nestedStructToJSONString(
								(List<?>) entry, pos, entrySchema);
						jsonString.append(nestedValue);

					}

				}

				if (i < entries.size() - 1) {
					jsonString.append(",");
				}

			}

			if (jsonString.toString().trim().length() > 1) {
				return jsonString.append("]").toString();
			} else {
				return "[]";
			}

		} catch (HCatException e) {
			throw new InterruptedException(e.getMessage());
		}

	}

	public static String mapToJSONString(HCatRecord record, int pos,
			HCatSchema schema) throws InterruptedException {

		StringBuilder jsonString = new StringBuilder();

		try {
			jsonString.append("{");

			HCatFieldSchema mapValueSchema = schema.get(pos)
					.getMapValueSchema().get(0);

			Map<?, ?> mapEntries = record.getMap(schema.get(pos).getName(),
					schema);
			if (mapEntries != null && !mapValueSchema.isComplex()
					&& !mapEntries.isEmpty()) {
				for (int i = 0; i < mapEntries.keySet().size(); i++) {
					String key = mapEntries.keySet().toArray()[i].toString();
					String value = mapEntries.get(key).toString();

					if (!key.trim().equals("")) {

						jsonString.append("\"");
						jsonString.append(key);

						if (value.startsWith("{") && value.contains(",")) {
							jsonString.append("\":");
							jsonString.append(value);
						} else {
							value = removeJsonFormatCharacters(value);
							jsonString.append("\":\"");
							jsonString.append(value);
							jsonString.append("\"");
						}

						if (i < mapEntries.keySet().size() - 1) {
							jsonString.append(",");
						}
					}

				}
			}

			return jsonString.append("}").toString();

		} catch (HCatException e) {
			throw new InterruptedException(e.getMessage());
		}

	}

	private static String nestedStructToJSONString(List<?> value, int pos,
			HCatFieldSchema schema) throws InterruptedException {

		StringBuilder jsonString = new StringBuilder();

		try {
			jsonString.append("{");
			for (int sf = 0; sf < schema.getStructSubSchema().size(); sf++) {
				String stuctKey = schema.getStructSubSchema().get(sf).getName();

				jsonString.append("\"");
				jsonString.append(stuctKey);

				if (value.get(sf) != null) {
					String structVal = removeJsonFormatCharacters(value.get(sf)
							.toString());
					jsonString.append("\":\"");
					jsonString.append(structVal);
					jsonString.append("\"");
				} else {
					jsonString.append("\":");
					jsonString.append("null");
				}

				if (sf != schema.getStructSubSchema().size() - 1) {
					jsonString.append(",");
				}

			}
			return jsonString.append("}").toString();
		} catch (HCatException e) {
			throw new InterruptedException(e.getMessage());
		}

	}

}
