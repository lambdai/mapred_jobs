package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class LeftJoinMapper extends
		Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	int[] keyColumnIndexes;
	int[] valueColumnIndexes;

	Row row;
	BytesWritable tkey;
	BytesWritable tvalue;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String join_using_columns = conf.get(Constant.JOIN_USING);
		String schema_str = conf.get(Constant.LEFT_JOIN_SCHEMA);
		Schema schema = new Schema("left");
		schema.parseAndSetRecordDescriptor(schema_str);
		row = Row.createBySchema(schema);
		keyColumnIndexes = Schema.columnIndexes(schema,
				SchemaUtils.parseColumns(join_using_columns));
		valueColumnIndexes = SchemaUtils.columnLeft(keyColumnIndexes, schema
				.getRecordDescriptor().size());
	}

	@Override
	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		row.readFieldsFromBytes(value);
		row.writeToBytes(tkey, keyColumnIndexes);
		row.writeToBytesWithLeftMark(tvalue, valueColumnIndexes);
		context.write(tkey, tvalue);
	}

}
