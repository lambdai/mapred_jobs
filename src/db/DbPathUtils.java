package db;

import org.apache.hadoop.fs.Path;

import db.table.Schema;

public class DbPathUtils {
	
	public final static Path TABLES_PATH = new Path("tables/");
	public static Path tablePath(Schema schema) {
		return tablePath(schema.getTableName());
	}

	public static Path tablePath(String tName) {
		Path ret = new Path(TABLES_PATH, tName);
		return ret;
	}
}
