package db.sql.aggregation;

import db.table.Field;
import db.table.IntField;

public class Average implements AggregationFunction {

	private int sum;
	private int count;
	@Override
	public void init() {
		sum = 0;
		count = 0;
	}

	@Override
	public void accept(Field f) {
		sum += ((IntField)f).getValue();
		count += 1;
	}

	@Override
	public Field submit() {
		return new IntField(sum/count);
	}

}
