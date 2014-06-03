package db.sql;

import java.util.ArrayList;
import java.util.List;

import db.table.Schema;
import db.table.UnsupportedOperation;

public class RowEvaluatorFactory implements EvaluatorFactory {
	private RowEvaluationClosure closure;
	
	public Evaluator create(BoolValue boolOperation) {
		return create(boolOperation.left, boolOperation.right, boolOperation.op);
	}
	
	public RowEvaluationClosure getCloseure() {
		return closure;
	}

	public void setClosure(RowEvaluationClosure closure) {
		this.closure = closure;
	}

	private Evaluator create(PredicateOperand left, PredicateOperand right,
			PredicateOp op) {
		RowEvaluator ret = new RowEvaluator();
		ret.setF1(makeReadable(left));
		ret.setF2(makeReadable(right));
		ret.setOp(op);
		return ret;
	}
	
	private FieldReadable makeReadable(PredicateOperand operand) {
		
		if(operand instanceof FieldOperand) {
			return (FieldReadable)operand; 
		} else if (operand instanceof TableDotColumn) {
			List<String> requiredColumn = new ArrayList<String>(1);
			requiredColumn.add(((TableDotColumn)operand).columnName);
			int[] idx = Schema.columnIndexes(closure.getSchema(), requiredColumn);
			IndexField f = new IndexField();
			f.setClosure(closure);
			f.setOffset(idx[0]);
			return f;
		}
		throw new UnsupportedOperation(String.format("Wrong Field Operand: %s", operand.toString()));
	}
}
