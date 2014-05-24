package db.sql;

import db.Constant;


public class BoolValue implements BoolExpr {
	
	PredicateOperand left;
	PredicateOperand right;
	PredicateOp op;
	
	public BoolValue() {
		
	}
	public BoolValue(PredicateOperand left, PredicateOperand right,
			PredicateOp op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}
	
	@Override
	public Evaluator createEvaluator(EvaluatorFactory factory) {
		return factory.create(this);
	}
	
	public String toString() {
		return "{" + Constant.BOOL_VALUE_EXPR + " "+ op.getStringId() + " " + left.toString() + " " + right.toString() + "}";
	}
	
	public int parseFieldsFromString(String str, int start, int end) {
		WhereParser p = new WhereParser(str, start, end);
		op = p.parsePredicateOp();
		left = p.parsePredicateOpereand();
		right = p.parsePredicateOpereand();
		return p.getCurrent() + 1; // the cursor should be the right parenthesis of {BVAL op left right}
	}
}
