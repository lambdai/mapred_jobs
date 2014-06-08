package dyc.tools;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceReader {
	static {
		Configuration conf = new Configuration();
		try {
			FileSystem.getFileSystemClass("file", conf);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
		;
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	public static void main(String args[]) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[0]);
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Reader reader = null;
		int nline = 0;
		int nprint = Integer.parseInt(args[1]);
		reader = new SequenceFile.Reader(fs, path, conf);
		while (reader.next(key, value)) {
			nline++;
			if (nline < nprint) {
				System.out.print("" + nline + ":\t");
				System.out.println(key.toString());
				System.out.println(value.toString());
			}

		}
		reader.close();
		System.out.println("total line:" + nline);

	}

}
