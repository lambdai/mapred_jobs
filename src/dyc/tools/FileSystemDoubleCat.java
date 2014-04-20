package dyc.tools;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemDoubleCat {
	public static void main(String[] args) throws IOException {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			in.seek(0);
			byte[] buffer = new byte[1024];
			try {
				in.readFully(0, buffer, 10, 200);
			} catch (IOException e) {
				
			}
			System.out.write(buffer, 10, 20);
			System.out.write(buffer, 0, 20);
		} finally {
			in.close();
		}
	}

}
