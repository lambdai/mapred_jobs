package db.sql.aggregation;

import db.table.Field;
import db.table.IntField;

public class Max implements AggregationFunction {

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

}