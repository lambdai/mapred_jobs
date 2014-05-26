package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class GroupByMapper extends
		Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
	// indexes for key
	int[] keyColIndxs;
	// indexes for value
	int[] valColIndxs;
	// Input row template
	Row row;
	// output key template
	BytesWritable tKey;
	// output value template
	BytesWritable tVal;

	@Override
	public void setup(Context cont) throws IOException, InterruptedException {
		Configuration conf = cont.getConfiguration();
		String gb_key_str = conf.get(Constant.AGG_COLUMNS);
		// get the input table schema
		String schema_str = conf.get(Constant.INPUT_TABLE_SCHEMA);
		// schema for the row
		Schema schema = new Schema("groupby");
		schema.parseAndSetRecordDescriptor(schema_str);
		row = Row.createBySchema(schema);
		// get the group-by-key indexes in the schema  
		keyColIndxs = Schema.columnIndexes(schema,
				SchemaUtils.parseColumns(gb_key_str));
		
		// get the group-by-value indexes in the schema
		valColIndxs = SchemaUtils.columnLeft(keyColIndxs, schema
				.getRecordDescriptor().size());
		tKey = new BytesWritable();
		tVal = new BytesWritable();
	}

	@Override
	public void map(BytesWritable key, BytesWritable value, Context cont)
			throws IOException, InterruptedException {
		row.readFieldsFromBytes(value);
		row.writeToBytes(tKey, keyColIndxs);
		row.writeToBytes(tVal, valColIndxs);
		cont.write(tKey, tVal);
	}
}
