package db.sql.aggregation;

import db.table.Field;

public interface AggregationFunction {
	
	public abstract void init();
	
	public abstract void accept(Field f);
	
	public abstract Field submit();

}
