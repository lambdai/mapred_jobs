package db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import db.table.IntField;
import db.table.JoinRowFactory;
import db.table.JoinedRow;
import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class JoinReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
	JoinedRow joinedRow;
	JoinRowFactory factory;
	Schema leftValueSchema;
	Schema rightValueSchema;
	BytesWritable tKey;
	BytesWritable tValue;
	
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String join_using_columns = conf.get(Constant.JOIN_USING);
		String schema_str = conf.get(Constant.JOIN_RESULT_SCHEMA);
		String lSchema = conf.get(Constant.LEFT_JOIN_SCHEMA);
		String rSchema = conf.get(Constant.RIGHT_JOIN_SCHEMA);
		
		Schema result_schema = new Schema("join_result");
		Schema leftSchema = new Schema("left");
		Schema rightSchema = new Schema("right");
		
		result_schema.parseAndSetRecordDescriptor(schema_str);
		leftSchema.parseAndSetRecordDescriptor(lSchema);
		rightSchema.parseAndSetRecordDescriptor(rSchema);
		joinedRow = JoinedRow.createBySchema(result_schema, leftSchema, rightSchema, join_using_columns);
		factory = new JoinRowFactory();
		tKey = Constant.EMPTY_BYTESWRITABLE;
		tValue = new BytesWritable();
	}
	
	protected void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		joinedRow.initByEquiColumns(key);
		
		List<Row> leftRows = new ArrayList<Row>();
		List<Row> rightRows = new ArrayList<Row>();
		List<Row> currentList;
		for(BytesWritable bytes: values) {
			IntField markField = factory.readOneFieldFromBytes(bytes);
			Row row;
			if(markField.equals(Row.fieldMarkLeft)) {
				currentList = leftRows;
				row = Row.createBySchema(leftValueSchema);
			} else {
				currentList = rightRows;
				row = Row.createBySchema(rightValueSchema);
			}
			factory.readRemaining(row);
			currentList.add(row);
		}
		
		for(Row left : leftRows) {
			joinedRow.setCursorOnLeft();
			joinedRow.push(left);
			for(Row right:rightRows) {
				joinedRow.setCursorOnRight();
				joinedRow.push(right);
				joinedRow.writeToBytes(tValue);
				context.write(tKey, tValue);
			}
		}
		
		
	}

}
