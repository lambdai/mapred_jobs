package db.sql.aggregation;

import db.table.Field;
import db.table.FieldType;

public interface AggregationFunction {
	
	public abstract void init();
	
	public abstract void accept(Field f);
	
	public abstract Field submit();
	
	public String getFunctionName();

	public FieldType outputFieldType();
	
}
