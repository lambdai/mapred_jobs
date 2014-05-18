package db.sql;

import java.util.Stack;

public class PredicateExprFactory {
	Stack<PredicateOperand> operandstack = new Stack<PredicateOperand>();
	Stack<PredicateOp> opstack = new Stack<PredicateOp>();
	
	public void accept(PredicateOp op) {
		opstack.push(op);
	}
	
	public void accept(PredicateOperand operand) {
		operandstack.push(operand);
	}
	
	public BoolExpr generatePredicate() {
		PredicateOperand right = operandstack.pop();
		PredicateOperand left = operandstack.pop();
		return new BoolValue(left, right, opstack.pop());
	}
}
