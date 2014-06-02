package dyc.tools;

import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;

import db.DbPathUtils;
import db.table.Field;
import db.table.IntField;
import db.table.Row;
import db.table.Schema;
import dyc.tools.TableGenerator.IRowGenerator;

public class TableDisplayer {
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

	static void printUsage() {
		System.out.println("Use TableDisplayer tableName nRows nCols [schema]");
	}

	public static void main(String[] args) throws Exception {
		String tableName = null;
		int nCols = 2;
		int nRows = 100;
		IRowGenerator generator = null;
		String schemaStr;
		Schema schema = null;
		try {
			tableName = args[0];
			nRows = Integer.parseInt(args[1]);
			nCols = Integer.parseInt(args[2]);
			if (args.length < 4) {
				schemaStr = new String();
				for (int i = 1; i <= nCols; i++) {
					schemaStr = schemaStr
							+ String.format(
									"{SimpleColumnDescriptor cname%d 1};", i);
				}
			} else {
				schemaStr = args[3];
			}
			schema = new Schema(tableName);
			schema.parseAndSetRecordDescriptor(schemaStr);
			nCols = schema.getRecordDescriptor().size();
		} catch (RuntimeException e) {
			printUsage();
			System.exit(1);
		}
		Row row = Row.createBySchema(schema);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path path = DbPathUtils.tablePath(tableName);
		BytesWritable key = new BytesWritable();
		BytesWritable value = new BytesWritable();
		FileStatus[] files = fs.listStatus(path, new PathFilter() {

			@Override
			public boolean accept(Path path) {
				return !path.getName().equals("_SUCCESS")
						&& !path.getName().equals("_logs");
			}

		});
		SequenceFile.Reader reader = null;
		for (FileStatus f : files) {
			reader = new SequenceFile.Reader(fs, f.getPath(), conf);
			while (reader.next(key, value)) {
				nRows--;
				if (nRows < 0) {
					reader.close();
					return;
				}
				row.readFieldsFromBytes(value);
				RowUtils.printIntFieldRow(row);
			}
			reader.close();
		}
	}
}
