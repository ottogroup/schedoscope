package org.schedoscope.export.outputschema;

import java.util.Map;

import org.apache.hive.hcatalog.data.schema.HCatSchema;

public class SchemaUtils {

	public static String[] getColumnNamesFromHcatInputSchema(
			HCatSchema inputSchema, Map<String, String> columnNameMapping) {

		Object[] hcatInputNames = inputSchema.getFieldNames().toArray();
		String[] outputColumnNames = new String[hcatInputNames.length + 1];

		for (int i = 0; i < hcatInputNames.length; i++) {
			String fieldName = hcatInputNames[i].toString();
			if (columnNameMapping.containsKey(fieldName)) {
				fieldName = columnNameMapping.get(fieldName);
			}
			outputColumnNames[i] = fieldName;
		}

		outputColumnNames[hcatInputNames.length] = "used_filter";
		return outputColumnNames;
	}

	public static String[] getColumnTypesFromHcatInputSchema(
			HCatSchema inputSchema, Map<String, String> columnNameMapping) {

		String[] fieldTypes = new String[inputSchema.getFieldNames().size() + 1];

		for (int i = 0; i < inputSchema.getFieldNames().size(); i++) {
			String type = columnNameMapping.get("string");
			if (!inputSchema.get(i).isComplex()) {
				if (inputSchema.get(i).getTypeString().toLowerCase() == null
						|| inputSchema.get(i).getTypeString().toLowerCase()
								.equals("null")) {
					type = columnNameMapping.get("tinyint");
				} else {
					type = columnNameMapping.get(inputSchema.get(i)
							.getTypeString().toLowerCase());
				}
			}
			fieldTypes[i] = type;
		}

		fieldTypes[inputSchema.getFieldNames().size()] = columnNameMapping
				.get("string");
		return fieldTypes;
	}

}
