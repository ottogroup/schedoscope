package org.schedoscope.export;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.schedoscope.export.outputformat.JdbcOutputWritable;
import org.schedoscope.export.outputschema.Schema;
import org.schedoscope.export.outputschema.SchemaFactory;

public class JdbcExportReducer extends
		Reducer<Text, NullWritable, JdbcOutputWritable, NullWritable> {

	private String[] columnTypes;
	private Map<String, String> preparedStatementTypeMapping;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);
		Schema outputSchema = SchemaFactory.getSchema(context
				.getConfiguration());
		columnTypes = outputSchema.getColumnTypes();
		preparedStatementTypeMapping = outputSchema
				.getPreparedStatementTypeMapping();

	}

	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {

		String[] line = key.toString().split("\t");

		if (line.length == columnTypes.length) {
			JdbcOutputWritable output = new JdbcOutputWritable(line,
					columnTypes, preparedStatementTypeMapping);
			context.write(output, NullWritable.get());
		}

	}

}
