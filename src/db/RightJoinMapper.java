package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class RightJoinMapper extends
		Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

	int[] keyColumnIndexes;
	int[] valueColumnIndexes;

	Row row;
	BytesWritable tKey;
	BytesWritable tValue;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String join_using_columns = conf.get(Constant.JOIN_USING);
		Schema schema = Schema.createSchema(conf.get(Constant.RIGHT_JOIN_SCHEMA));
		row = Row.createBySchema(schema);
		keyColumnIndexes = Schema.columnIndexes(schema,
				SchemaUtils.parseColumns(join_using_columns));
		valueColumnIndexes = SchemaUtils.columnLeft(keyColumnIndexes, schema
				.getRecordDescriptor().size());
		tValue = new BytesWritable();
		tKey = new BytesWritable();
	}

	@Override
	public void map(BytesWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		row.readFieldsFromBytes(value);
		row.writeToBytes(tKey, keyColumnIndexes);
		row.writeToBytesWithRightMark(tValue, valueColumnIndexes);
		context.write(tKey, tValue);
	}

}
