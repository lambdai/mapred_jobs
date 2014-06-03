package db.sql.aggregation;

import java.util.HashMap;
import java.util.Map;

import db.table.RowFormatException;

public class AggregationUtils {
	
	public static Map<String, Class<? extends AggregationFunction>> functions = new HashMap<String, Class<? extends AggregationFunction>>();
	
	public static synchronized void put(String str, Class<? extends AggregationFunction> clazz) {
		functions.put(str, clazz);
	}
	
	public static synchronized Class<? extends AggregationFunction> get(String str) {
		return functions.get(str);
	}
	
	public static synchronized AggregationFunction createFunctionInstance(String fName) {
		AggregationFunction ret;
		try {
			//System.out.println(fName);
			//System.out.println(get(fName).getSimpleName());
			ret = (AggregationFunction) get(fName).getConstructor().newInstance();
		} catch (Exception e) {
			throw new RowFormatException(fName);
		}
		return ret;
	}
	
	static {
		try {
			Class.forName("db.sql.aggregation.Sum");
			Class.forName("db.sql.aggregation.Average");
			Class.forName("db.sql.aggregation.Max");
			Class.forName("db.sql.aggregation.Min");
			Class.forName("db.sql.aggregation.Count");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
