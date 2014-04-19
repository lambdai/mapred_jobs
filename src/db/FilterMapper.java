package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	RowFilter f;
	

	public void setup(Context context) {
		Configuration conf = context.getConfiguration(); 
        String columnId = conf.get(Constant.COLUMN_ID);
        String destValue = conf.get(Constant.DEST_VALUE);        
        f = new EqualFilter(Integer.parseInt(columnId), Integer.parseInt(destValue));
        
    }
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] strRow = line.split(" ");
		if(f.filtered(strRow)) {
			context.write(value, Constant.EMPTY_TEXT);
		}
		
	}
	
}
