package db.sql.aggregation;

import db.table.Field;
import db.table.IntField;

public class Count implements AggregationFunction {

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

}