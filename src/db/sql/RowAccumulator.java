package db.sql;

import java.util.ArrayList;
import java.util.List;

import db.sql.aggregation.AggregationFunction;
import db.table.Field;
import db.table.Row;

public class RowAccumulator {

	List<Closure> closures = new ArrayList<Closure>();
	Row row;
	
	class Closure {
		
		AggregationFunction func;
		int iColumn;
		
		public Closure(AggregationFunction func, int iColumn) {
			super();
			this.func = func;
			this.iColumn = iColumn;
		}
		void init() {
			func.init();
		}
		
		void accept() {
			func.accept(row.getFields()[iColumn]);
		}
		
		Field submit() {
			return func.submit();
		}
		
	}
	
	public RowAccumulator(AggregationFunction[] funcs, int[] cols) {
		int len = funcs.length;
		
		for(int i = 0; i< len; i++) {
			closures.add(new Closure(funcs[i], cols[i]));
		}
	}
	
	public void init() {
		for(Closure closure : closures) {
			closure.init();
		}
	}

	public void accept(Row row) {
		this.row = row;
		for(Closure closure : closures) {
			closure.accept();
		}
	}
	
	public Row submitRow() {
		Row ret = new Row();
		Field[] fs = new Field[closures.size()];
		int i = 0;
		for(Closure closure : closures) {
			fs[i++] = closure.submit();
		}
		ret.setFields(fs);
		return ret;
	}

}
