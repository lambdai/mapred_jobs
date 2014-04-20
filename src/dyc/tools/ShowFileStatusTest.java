package dyc.tools;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
public class ShowFileStatusTest {
	private MiniDFSCluster cluster;
	private FileSystem fs;
	
	@Before
	public void setUp() throws IOException {
		Configuration conf = new Configuration();
		if (System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
		}
		cluster = new MiniDFSCluster(conf, 1, true, null);
		fs = cluster.getFileSystem();
		OutputStream out= fs.create(new Path("/dir/file"));
		out.write("content".getBytes("UTF-8"));
		out.close();
	}
	
	@After
	public void tearDonw() throws IOException {
		if(fs != null) {
			fs.close();
		}
		if(cluster != null) {
			cluster.shutdown();
		}
	}
	
	@Test(expected = FileNotFoundException.class) 
	public void throwsFileNotFoundForNonExistentFile() throws 	IOException {
		fs.getFileStatus(new Path("no-such-file"));
	}
	
	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path("/dir/file");
		FileStatus stat = fs.getFileStatus(file);
		assertThat(stat.getPath().toUri().getPath(), is("/dir/file"));
	}
	
}
