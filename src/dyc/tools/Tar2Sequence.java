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
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;

public class Tar2Sequence {

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

		if (args.length == 0 || args[0].equals("--help")) {
			System.out.println("Usage: "
					+ Tar2Sequence.class.getCanonicalName() + "input_tar_file"
					+ " output_dir" + " classifier" + " chunksize");
		}
		Configuration conf = new Configuration();
		Text key = new Text();

		Text value = new Text();
		SequenceFile.Writer writer = null;

		String outputDir = args[1];
		String classifier = args[2];
		int maxChunkSize = Integer.parseInt(args[3]);
		int currentChunkSize = 0;
		int currentChunkIndex = 1;
		writer = SequenceFile.createWriter(
				conf,
				SequenceFile.Writer.compression(CompressionType.NONE),
				Writer.file(new Path(outputDir + "/chunk"
						+ String.format("%05d", currentChunkIndex))),
				SequenceFile.Writer.keyClass(key.getClass()),
				SequenceFile.Writer.valueClass(value.getClass()));

		TarArchiveInputStream tarIn = new TarArchiveInputStream(

		new BufferedInputStream(new FileInputStream(args[0]))

		);
		TarArchiveEntry tarEntry = tarIn.getNextTarEntry();
		while (tarEntry != null) {
			if (tarEntry.isDirectory()) {
				// destPath.mkdirs();
			} else {
				if (currentChunkSize > maxChunkSize) {
					writer.close();
					currentChunkSize = 0;
					currentChunkIndex++;
					writer = SequenceFile.createWriter(conf,
							SequenceFile.Writer
									.compression(CompressionType.NONE), Writer
									.file(new Path(outputDir
											+ "/chunk"
											+ String.format("%05d",
													currentChunkIndex))),
							SequenceFile.Writer.keyClass(key.getClass()),
							SequenceFile.Writer.valueClass(value.getClass()));
				}
				System.out.println(tarEntry.getName());
				key.set(classifier + "/" + tarEntry.getName().replace('/', '_'));
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
				currentChunkSize += (key.getLength() + value.getLength());

				// we can not close breader
				// breader.close();

			}
			tarEntry = tarIn.getNextTarEntry();
		}

	}
}
