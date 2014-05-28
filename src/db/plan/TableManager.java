package db.plan;

import java.util.HashMap;
import java.util.Map;

import db.table.Schema;

public class TableManager {
	
	static Map<String, Schema> tables = new HashMap<String, Schema>();
	
	public static synchronized Schema getSchema(String tName) {
		if(tName.startsWith(TempTableManager.tmpPrefix)) {
			return TempTableManager.getSchema(tName);
		} else {
			return tables.get(tName);
		}
	}
	
	public static synchronized Schema createTempTable() {
		return TempTableManager.createTempTable();
	}
	
	public static synchronized void putSchema (String tName, Schema schema) {
		tables.put(tName, schema);
	}
	
	public static synchronized void putSchema (Schema schema) {
		tables.put(schema.getTableName(), schema);
	}
	
	
	
}
