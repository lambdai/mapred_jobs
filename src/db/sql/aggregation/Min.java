package db.sql.aggregation;

import db.table.Field;
import db.table.FieldType;
import db.table.IntField;

public class Min implements AggregationFunction {
	
	public final static String functionName = "MIN";

	private int min;
	@Override
	public void init() {
		min = Integer.MAX_VALUE;
	}

	@Override
	public void accept(Field f) {
		IntField i = (IntField)f;
		min = Math.min(min, i.getValue());
	}

	@Override
	public Field submit() {
		return new IntField(min);
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
		AggregationUtils.put(functionName, Min.class);
	}
	

}