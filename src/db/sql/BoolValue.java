package db.sql;

public class BoolValue implements BoolExpr {
	
	PredicateOperand left;
	PredicateOperand right;
	PredicateOp op;
	public BoolValue(PredicateOperand left, PredicateOperand right,
			PredicateOp op) {
		this.left = left;
		this.right = right;
		this.op = op;
	}
	
	

}
