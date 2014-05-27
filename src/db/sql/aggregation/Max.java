package db.sql.aggregation;

import db.table.Field;
import db.table.IntField;

public class Max implements AggregationFunction {

	public final static String functionName = "MAX";
	
	private int max;
	@Override
	public void init() {
		max = Integer.MIN_VALUE;
	}

	@Override
	public void accept(Field f) {
		IntField i = (IntField)f;
		max = Math.max(max, i.getValue());
	}

	@Override
	public Field submit() {
		return new IntField(max);
	}

	@Override
	public String getFunctionName() {
		return  functionName;
	}
	
	static {
		AggregationUtils.put(functionName, Max.class);
	}

}