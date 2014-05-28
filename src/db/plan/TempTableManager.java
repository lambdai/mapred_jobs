package db.plan;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import db.table.Schema;

public class TempTableManager {
	
	private static AtomicInteger index = new AtomicInteger(0);
	public static final String tmpPrefix = "__tmp";
	
	static Map<String, Schema> tmpTables = new HashMap<String, Schema>();
	
	public static synchronized Schema createTempTable() {
		int idx = index.incrementAndGet();
		String tableName = tmpPrefix+idx;
		Schema ret = new Schema(tmpPrefix+idx);
		tmpTables.put(tableName, ret);
		return ret;
	}
	
	public static synchronized Schema getSchema(String tName) {
		return tmpTables.get(tName);
	}
}
