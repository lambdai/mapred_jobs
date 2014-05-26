package db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggegationMapper extends Mapper<LongWritable, Text, Text, Text> {

	int [] aggIds;
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration(); 
        String aggId = conf.get(Constant.AGG_COLUMNS);       
        String strAggId[] = aggId.split(" ");
        aggIds = new int[strAggId.length];
        for(int i = 0; i < strAggId.length; i ++ ) {
        	aggIds[i] = Integer.parseInt(strAggId[i]);
        }
    }
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String line = value.toString();
		String[] strRow = line.split(" ");
		Text sendkey = MapKeyGenerator.genKey(strRow, aggIds);
		context.write(sendkey, value);
	}
	
}
