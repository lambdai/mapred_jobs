package db.sql;

import db.Constant;


public class OrExpr implements BoolExpr {
	BoolExpr left;
	BoolExpr right;

	public BoolExpr getLeft() {
		return left;
	}

	public void setLeft(BoolExpr left) {
		this.left = left;
	}

	public BoolExpr getRight() {
		return right;
	}

	public void setRight(BoolExpr right) {
		this.right = right;
	}

	@Override
	public Evaluator createEvaluator(EvaluatorFactory factory) {
		Evaluator l = left.createEvaluator(factory);
		Evaluator r = right.createEvaluator(factory);
		CombinedEvaluator eval = new CombinedEvaluator() {

			@Override
			public boolean evalutate() {
				for(Evaluator e : getSubEvalutors()) {
					if(e.evalutate()) {
						return true;
					}
				}
				return false;
			}
			
		};
		eval.pushEvalutor(l);
		eval.pushEvalutor(r);
		return eval;
		
	}
	
	public String toString() {
		return "{" + Constant.OR_EXPR + " " + left.toString() + " " + right.toString() + "}";
	}

	public int parseFieldsFromString(String str, int start, int end) {
		WhereParser p = new WhereParser(str, start, end);
		left = p.parseBoolExpr();
		p.incCurrent();
		right = p.parseBoolExpr();
		return p.getCurrent(); // the cursor should be the right parenthesis of {OrExpr left right}
	}
}
