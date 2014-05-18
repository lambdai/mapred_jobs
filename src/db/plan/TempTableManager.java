package db.plan;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import db.table.Schema;

public class TempTableManager {
	
	private AtomicInteger index = new AtomicInteger(0);
	public final String tmpPrefix = "__tmp";
	
	Map<String, Schema> tmpTables = new HashMap<String, Schema>();
	
	public Schema createTempTable() {
		int idx = index.incrementAndGet();
		String tableName = tmpPrefix+idx;
		Schema ret = new Schema(tmpPrefix+idx);
		tmpTables.put(tableName, ret);
		return ret;
	}
}
