package db.sql.aggregation;

import db.table.Field;
import db.table.IntField;

public class Sum implements AggregationFunction {

	public final static String functionName = "SUM";
	private int sum;
	@Override
	public void init() {
		sum = 0;
	}

	@Override
	public void accept(Field f) {
		IntField i = (IntField)f;
		sum += i.getValue();
	}

	@Override
	public Field submit() {
		return new IntField(sum);
	}

	@Override
	public String getFunctionName() {
		return  functionName;
	}
	
	static {
		AggregationUtils.put(functionName, Sum.class);
	}
}
