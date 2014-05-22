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

	/* Basically this setup method will preparpare for map method
	 * The method will saparate the groupBy key word and store the index into keyColIndxs
	 * And the rest of key words will be stored into valColIndxs, that's it.
	 */
	
	@Override
	public void setup(Context cont) throws IOException, InterruptedException{
		Configuration conf= cont.getConfiguration();
		String gb_key_str=conf.get("Constant.AGG_COLUMN_ID"); // guessing these are keys for group by
		String schema_str=conf.get("group by schema"); // get the schema
		Schema schema = new Schema("whatever the schema is"); // schema for the row
		schema.parseAndSetRecordDescriptor(schema_str); //convert the groupBy schema into a List (recordDescriptor）
		row=Row.createBySchema(schema);// create the row with the schema but initschema??
	
        /* this schema comes from Master/scr/db/table/schema.java
		 * convert gb_key_str to a LIST as the second parameter
		 * keyColIndxs will be updated with the index of group by key words 
		 * such as if say GROUPBY name, age, then return 1st 2nd column.
		 * the same thing for valColIndex I think :D ，，，
		 */

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
	
