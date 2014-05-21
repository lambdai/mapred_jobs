package db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import db.table.IntField;
import db.table.JoinRowFactory;
import db.table.Row;
import db.table.Schema;

public class JoinReducer extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
	Row joinedRow;
	JoinRowFactory factory;
	Schema leftValueSchema;
	Schema rightValueSchema;
	int[] lkeyColumnIndexes;
	int[] lvalueColumnIndexes;
	int[] rkeyColumnIndexes;
	int[] rvalueColumnIndexes;
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		String join_using_columns = conf.get(Constant.JOIN_USING);
		String schema_str = conf.get(Constant.JOIN_RESULT_SCHEMA);
		String lSchema = conf.get(Constant.LEFT_JOIN_SCHEMA);
		String rSchema = conf.get(Constant.RIGHT_JOIN_SCHEMA);
		
		Schema result_schema = new Schema("join_result");
		result_schema.parseAndSetRecordDescriptor(schema_str);
		joinedRow = Row.createBySchema(result_schema);
		factory = new JoinRowFactory();
		
		
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
			for(Row right:rightRows) {
				joinedRow.init();
				joinedRow.push(left);
				joinedRow.push(right);
				context.write(null, joinedRow);
			}
		}
		
		
	}

}
