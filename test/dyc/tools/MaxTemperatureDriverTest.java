package dyc.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import dyc.maxtemperature.MaxTemperatureDriver;

public class MaxTemperatureDriverTest {
	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		Path input = new Path("input/ncdc/micro");
		Path output = new Path("output");
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true); // delete old output
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		int exitCode = driver.run(new String[] { input.toString(),
				output.toString() });
		assertThat(exitCode, is(0));
		// checkOutput(conf, output);
	}

}
