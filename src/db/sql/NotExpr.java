package db.sql;

import db.Constant;


public class NotExpr implements BoolExpr {
	BoolExpr child;

	public BoolExpr getChild() {
		return child;
	}

	public void setChild(BoolExpr child) {
		this.child = child;
	}

	@Override
	public Evaluator createEvaluator(EvaluatorFactory factory) {
		Evaluator e = child.createEvaluator(factory);
		CombinedEvaluator eval = new CombinedEvaluator() {

			@Override
			public boolean evalutate() {
				return !getSubEvalutors().get(1).evalutate();
			}
			
		};
		eval.pushEvalutor(e);
		return eval;
	}

	public String toString() {
		return "{" + Constant.NOT_EXPR + " " + child.toString() + "}";
	}
	
	public int parseFieldsFromString(String str, int start, int end) {
		WhereParser p = new WhereParser(str, start, end);
		child = p.parseBoolExpr();
		return p.getCurrent() + 1; // the cursor should be the right parenthesis of {Not child}
	}
}
