package db.sql.aggregation;

import java.util.HashMap;
import java.util.Map;

public class AggregationUtils {
	
	public static Map<String, Class> functions = new HashMap<String, Class>();
	
	public static synchronized void put(String str, Class clazz) {
		functions.put(str, clazz);
	}
	
	public static synchronized Class get(String str) {
		return functions.get(str);
	}
	
	

}
