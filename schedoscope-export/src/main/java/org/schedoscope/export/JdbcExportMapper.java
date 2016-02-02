package org.schedoscope.export;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema.Category;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;
import org.schedoscope.export.utils.ComplexTypeUtils;

public class JdbcExportMapper extends
		Mapper<WritableComparable<?>, HCatRecord, Text, NullWritable> {

	private static final Log LOG = LogFactory.getLog(JdbcExportMapper.class);
	private static final String FIELDSEPARATOR = "\t";
	private HCatSchema inputSchema;
	private String inputFilter;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		inputSchema = HCatInputFormat
				.getTableSchema(context.getConfiguration());

		Schema outputSchema = SchemaFactory.getSchema(context
				.getConfiguration());
		inputFilter = outputSchema.getFilter();

		LOG.info("Used Filter: " + inputFilter);

	}

	@Override
	protected void map(WritableComparable<?> key, HCatRecord value,
			Context context) throws IOException, InterruptedException {

		StringBuilder output = new StringBuilder();

		for (int i = 0; i < value.size(); i++) {
			String fieldValue = "NULL";

			if (inputSchema.get(i).isComplex()
					&& inputSchema.get(i).getCategory() == Category.STRUCT
					&& value.getStruct(inputSchema.get(i).getName(),
							inputSchema) != null) {
				fieldValue = validateJsonFormat(ComplexTypeUtils
						.structToJSONString(value, i, inputSchema));
			} else if (inputSchema.get(i).isComplex()
					&& inputSchema.get(i).getCategory() == Category.ARRAY
					&& value.getList(inputSchema.get(i).getName(), inputSchema) != null) {
				fieldValue = validateJsonFormat(ComplexTypeUtils
						.arrayToJSONString(value, i, inputSchema));
			} else if (inputSchema.get(i).isComplex()
					&& inputSchema.get(i).getCategory() == Category.MAP
					&& value.getMap(inputSchema.get(i).getName(), inputSchema) != null) {
				fieldValue = validateJsonFormat(ComplexTypeUtils
						.mapToJSONString(value, i, inputSchema));
			} else {

				if (value.get(i) != null) {
					fieldValue = value.get(i).toString();
				}
			}
			output.append(fieldValue);
			output.append(FIELDSEPARATOR);

		}

		output.append(inputFilter);
		context.write(new Text(output.toString()), NullWritable.get());

	}

	private String validateJsonFormat(String jsonString) {

		try {
			if (jsonString.startsWith("{")) {
				new JSONObject(jsonString);
			} else if (jsonString.startsWith("[")) {
				new JSONArray(jsonString);
			}
			return jsonString;
		} catch (JSONException e) {
			LOG.warn(jsonString + " is not valide.");
			return jsonString;

		} catch (NumberFormatException e) {
			LOG.warn(jsonString + " is not valide.");
			return jsonString;

		}
	}

}
