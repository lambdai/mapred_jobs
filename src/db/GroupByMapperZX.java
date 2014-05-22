import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import db.table.*;

public class GroupByMapperZX extends Mapper<BytesWritable,BytesWritable,BytesWritable,BytesWritable>{
	int[] keyColIndxs;
	int[] valColIndxs;	// indexes for key and values in output
	Row row;	//	one row
	BytesWritable tKey;	
	BytesWritable tVal;	// key and values in BytesWritable
	
	@Override
	public void setup(Context cont) throws IOException, InterruptedException{
		Configuration conf= cont.getConfiguration();
		String gb_key_str=conf.get("Constant.AGG_COLUMN_ID"); // guessing these are keys for group by
		String schema_str=conf.get("group by schema"); // get the schema
		Schema schema = new Schema("whatever the schema is"); // schema for the row
		schema.parseAndSetRecordDescriptor(schema_str); //convert the groupBy schema into a List (recordDescriptor)
		row=Row.createBySchema(schema);// create the row with the schema but initschema??
		keyColIndxs=Schema.columnIndexes(schema, SchemaUtils.parseColumns(gb_key_str));	
		valColIndxs=SchemaUtils.columnLeft(keyColIndxs, schema.getRecordDescriptor().size());	//get the column indexes of the group by keys and the values
		tKey=new BytesWritable();
		tVal=new BytesWritable();
	}
	
	@Override
	public void map(BytesWritable key, BytesWritable value, Context cont) throws IOException, InterruptedException{
		row.readFieldsFromBytes(value);
		row.writeToBytes(tKey, keyColIndxs); // convert into bytesWritable
		row.writeToBytes(tVal, valColIndxs);
		cont.write(tKey,tVal);	// done
	}
}
	
