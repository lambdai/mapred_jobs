package db.sql.aggregation;

import java.lang.reflect.InvocationTargetException;
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
			ret = (AggregationFunction) get(fName).getConstructor().newInstance();
		} catch (InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new RowFormatException(fName);
		}
		return ret;
	}
	
	

}
