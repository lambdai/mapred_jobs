package db.sql.aggregation;

import db.table.Field;
import db.table.FieldType;
import db.table.IntField;

public class Count implements AggregationFunction {

	public final static String functionName = "COUNT";
	
	private int count;
	@Override
	public void init() {
		count = 0;
	}

	@Override
	public void accept(Field f) {
		count ++;
	}

	@Override
	public Field submit() {
		return new IntField(count);
	}

	@Override
	public String getFunctionName() {
		return  functionName;
	}
	
	@Override
	public FieldType outputFieldType() {
		return FieldType.IntType;
	}
	
	static {
		AggregationUtils.put(functionName, Count.class);
	}
}