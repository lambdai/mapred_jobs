package dyc.tools;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import dyc.maxtemperature.MaxTemperatureMapper;

public class MaxTemperatureMapperTest {

	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		Text value = new Text("0043011990999991950051518004+68750+023550FM-12+0382" +
				// Year ^^^^
		"99999V0203201N00261220001CN9999999N9-00111+99999999999");
		new MapDriver<LongWritable, Text, Text, IntWritable>()
		.withMapper(new MaxTemperatureMapper())
		.withInput(new Pair<LongWritable, Text>(new LongWritable(1),value))
		.withOutput(new Text("1950"), new IntWritable(-11))
		.runTest();

	}
}
