package db.sql;

import db.table.Row;
import db.table.Schema;

public class SelectionPipe {
	
	RowEvaluationClosure rowClosure;
	Evaluator whereEvaluator = null;

	public SelectionPipe(String whereStr, Schema schema) {
		rowClosure = new RowEvaluationClosure();
		RowEvaluatorFactory rowEvalFactory = new RowEvaluatorFactory();
		rowEvalFactory.setClosure(rowClosure);
		rowClosure.setSchema(schema);
		if (whereStr != null && whereStr.length() != 0) {
			BoolExpr whereExpr = new WhereParser(whereStr).parseBoolExpr();
			whereEvaluator = whereExpr.createEvaluator(rowEvalFactory);
		}
	}

	public void write(Row row) {
		rowClosure.setRow(row);
	}

	public Row read() {
		if (whereEvaluator == null || whereEvaluator.evalutate()) {
			return rowClosure.getRow();
		}
		return null;
	}
}
