package db;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupByMapper extends Mapper<LongWritable, Text, Text, Text> {

	RowFilter f;
	List<Integer> sendKeyIndex;
	List<Integer> leftKeyIndex;

	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		// get conf and setup
		String schema = conf.get("tableschema"); // return "name, age, salary"
		String sendkey = conf.get("groupbykey"); // return "name, age"
		// questions: 1. where are these strings? 2. the purpose is build up
		// sendkeyIndex?

		String[] schemaArr = schema.split(","); // {name, age, salary}

		String[] sendKeyArr = sendkey.split(","); // {name, age}

		// {a, b, c, d, e, f}
		// {a, d}
		int flag = 0;
		for (int i = 0; i < schemaArr.length; i++) {
			for (int j = 0; j < sendKeyArr.length; j++) {
				if (schemaArr[i] == sendKeyArr[j]) {
					sendKeyIndex.add(j + 1); // eg: j = 0, but first column
					flag = 1; // found and store in sendKeyIndex list
					break; // compare with next column in schema
				}
			}

			// if flag is not 1 -> not found, store in leftKeyIndex list
			if (flag != 1) {
				leftKeyIndex.add(i + 1);
			}
		}

		String columnId = conf.get(Constant.COLUMN_ID);
		String destValue = conf.get(Constant.DEST_VALUE);
		f = new EqualFilter(Integer.parseInt(columnId),
				Integer.parseInt(destValue));

	}

	@Override
	// value is each row
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] strRow = line.split(" ");
		if (f.filtered(strRow)) {
			context.write(value, Constant.EMPTY_TEXT);
		}

		// value is "v1 v2 v3", or "1 2 3"
		// "v1 v2" -> "v3", "1 2" -> "3"
		String columnString = value.toString();
		String[] columns = columnString.split(" "); 

		String sendKey = "";
		String leftColumns = "";
		
		for (int i : sendKeyIndex) {
			sendKey.concat(columns[sendKeyIndex.get(i)]);
		}
		for (int i : leftKeyIndex) {
			leftColumns.concat(columns[leftKeyIndex.get(i)]);
		}

		context.write(new Text(sendKey), new Text(leftColumns));
	}

}
