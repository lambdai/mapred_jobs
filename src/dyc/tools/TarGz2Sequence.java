package dyc.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TarGz2Sequence {

	public static Charset getCharset() {
		return Charsets.UTF_8;
	}

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

	public static void main(String args[]) throws FileNotFoundException,
			IOException {
		// FileInputStream fin = new FileInputStream("~/b.tar.gz");
		// BufferedInputStream in = new BufferedInputStream(fin);
		// FileOutputStream out = new FileOutputStream("archive.tar");
		// GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1]);
		Text key = new Text(args[1]);

		Text value = new Text();
		SequenceFile.Writer writer = null;
		writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
				value.getClass());

		TarArchiveInputStream tarIn = new TarArchiveInputStream(

		new BufferedInputStream(new FileInputStream(args[0]))

		);
		TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
		while (tarEntry != null) {
			if (tarEntry.isDirectory()) {
				// destPath.mkdirs();
			} else {
				System.out.println(tarEntry.getName());
				BufferedReader breader = new BufferedReader(
						new InputStreamReader(tarIn, getCharset()));
				StringBuilder sb = new StringBuilder();
				String line;
				while (true) {
					line = breader.readLine();
					if (line != null) {
						sb.append(line).append('\n');
					} else {
						break;
					}
				}
				value.set(sb.toString());
				writer.append(key, value);
				// we can not close breader
				// breader.close();

			}
			tarEntry = tarIn.getNextTarEntry();
		}

	}
}
