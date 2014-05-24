import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import db.Constant;	//need to be removed at submitting 
import db.table.*;

public class GroupByReducerZX extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
	Row outRow;	// the row for reduce output
	
	BytesWritable outKey;	
	BytesWritable outValue;	// the key & value for output
	int sumKeyIndex;	
	int avgKeyIndex;	// Q: an array or one int?

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();	//	get config
		
		String origin_schema_str=conf.get("THE SCHEMA NAME OF INPUT FOR GBMAPPER"); // get the schema of the input TABLE! 
		String avg_key_str = conf.get("THE AVG COLUMN");	// get the column name for avg
		String gb_key_str = conf.get("Constant.AGG_COLUMN_ID");	// get the group-by column name
		String sum_key_str = conf.get("THE COLUMN SUMMED ON");	//	get the column name for sum
		String[] gbKeys=gb_key_str.split(Constant.COLUMN_SPLIT);	//	parse the key string into separate field string
		
		for(String key: gbKeys){	// remove the gb keys from the original schema string
			origin_schema_str=origin_schema_str.replaceAll(key, "");
		}
		
		String out_schema_str = gb_key_str+Constant.COLUMN_SPLIT+origin_schema_str;	 // Q: get the result schema string, i.e., key+value, will this work? 
																					// what if there were two consecutive delimiters
		Schema outSchema=new Schema(out_schema_str);	// the schema of the output row
		
		outSchema.parseAndSetRecordDescriptor(out_schema_str);	// 
		outRow=Row.createBySchema(outSchema);	// Q: create the row, but will this work?
		outKey = Constant.EMPTY_BYTESWRITABLE;	// empty key?
		outValue = new BytesWritable();	// the whole row or just the remaining fields?
		
		sumKeyIndex=Schema.columnIndexes(outSchema, SchemaUtils.parseColumns(sum_key_str))[0];		
		avgKeyIndex=Schema.columnIndexes(outSchema, SchemaUtils.parseColumns(avg_key_str))[0];	//	Q: get the indexes of sum/avg column in the output row, will it work?
	}
	
	@Override
	protected void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		Field[] fields;	//	holding the fields in the row
		int sum=0;
		int avg=0;
		int count=0;
		IntField sumField=new IntField(sum);
		IntField avgField=new IntField(avg);
		for(BytesWritable value: values){	// just output the row without any calculation
			outRow.readFieldsFromBytes(key);
			outRow.readFieldsFromBytes(value);
			fields = outRow.getFields();	// get the fields, but how to do calculations on them
			sum	+= ((IntField) fields[sumKeyIndex]).getValue();	// sum
			count++;
		}
		avg=sum/count;	// avg
		avgField.setValue(avg);	
		sumField.setValue(sum);	// set values for fields
		outRow.setField(sumField, sumKeyIndex);
		outRow.setField(avgField, avgKeyIndex);	// set fields for row
		outRow.writeToBytes(outValue);
		context.write(outKey, outValue);	//	output
	}

}
