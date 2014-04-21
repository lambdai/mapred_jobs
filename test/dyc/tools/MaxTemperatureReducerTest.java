package dyc.tools;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import dyc.maxtemperature.MaxTemperatureReducer;

public class MaxTemperatureReducerTest {
	@Test
	public void returnsMaxiumumIntegerInValues() throws IOException,
			InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
				.withReducer(new MaxTemperatureReducer())
				.withInputKey(new Text("1950"))
				.withInputValues(
						Arrays.asList(new IntWritable(10), new IntWritable(5)))
				.withOutput(new Text("1950"), new IntWritable(10)).runTest();
	}
}
