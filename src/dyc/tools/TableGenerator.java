package dyc.tools;

import java.net.URL;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

import db.DbPathUtils;
import db.table.Field;
import db.table.IntField;
import db.table.Row;
import db.table.Schema;

public class TableGenerator {
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

	interface IRowGenerator {
		void writeRow(Row row);
	}

	static class SequenceGenerator implements IRowGenerator {
		int current, step;

		SequenceGenerator(int from, int step) {
			this.current = from;
			this.step = step;
		}

		@Override
		public void writeRow(Row row) {
			Field fields[] = row.getFields();
			for (int i = 0; i < fields.length; i++) {
				fields[i] = new IntField(current);
			}
			current += step;
		}
	}

	static class RandomGenerator implements IRowGenerator {
		int low, high;
		Random r;

		RandomGenerator(int low, int high) {
			this.low = low;
			this.high = high;
			r = new Random(0L);
		}

		@Override
		public void writeRow(Row row) {
			Field fields[] = row.getFields();
			for (int i = 0; i < fields.length; i++) {
				fields[i] = new IntField(r.nextInt(high - low) + low);
			}
		}

	}

	static void printUsage() {
		System.out.println("Use TableGenerator tableName nRows nCols ( [random low high] | [sequence from step] )");
	}

	public static void main(String[] args) throws Exception {
		String tableName = null;
		int nCols = 2;
		int nRows = 100;
		IRowGenerator generator = null;
		try {
			tableName = args[0];
			nRows = Integer.parseInt(args[1]);
			nCols = Integer.parseInt(args[2]);
			if (args[3].equals("random")) {
				generator = new RandomGenerator(Integer.parseInt(args[4]),
						Integer.parseInt(args[5]));
			} else if (args[3].equals("sequence")) {
				generator = new SequenceGenerator(Integer.parseInt(args[5]),
						Integer.parseInt(args[4]));
			} else {
				throw new RuntimeException("wrong generator type");
			}
		} catch (RuntimeException e) {
			printUsage();
			System.exit(1);
		}

		Schema schema = new Schema(tableName);
		String schemaStr = new String();
		for (int i = 1; i <= nCols; i++) {
			schemaStr = schemaStr + String.format("{SimpleColumnDescriptor cname%d 1};", i);
		}
		schema.parseAndSetRecordDescriptor(schemaStr);
		Row row = Row.createBySchema(schema);

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = DbPathUtils.tablePath(tableName);
		BytesWritable key = new BytesWritable();
		BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(),
					value.getClass());
			for (int i = 0; i < nRows; i++) {
				generator.writeRow(row);
				row.writeToBytes(value);
				RowUtils.printIntFieldRow(row);
				writer.append(key, value);
			}
		} finally {
			IOUtils.closeStream(writer);
		}

	}

}
