package db.sql;

import db.table.Field;

public class IndexField implements PredicateOperand, FieldReadable {

	private int offset;
	
	RowEvaluationClosure closure;
	
	public Field getF() {
		return closure.getRow().getFields()[offset];
	}
	

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public RowEvaluationClosure getClosure() {
		return closure;
	}

	public void setClosure(RowEvaluationClosure closure) {
		this.closure = closure;
	}

	
}
