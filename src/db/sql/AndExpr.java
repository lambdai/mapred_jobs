package db.sql;

import db.Constant;


public class AndExpr implements BoolExpr {
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
					if(!e.evalutate()) {
						return false;
					}
				}
				return true;
			}
			
		};
		eval.pushEvalutor(l);
		eval.pushEvalutor(r);
		return eval;
		
	}

	public String toString() {
		return "{" + Constant.AND_EXPR + " " + left.toString() + " " + right.toString() + "}";
	}
	
	public int parseFieldsFromString(String str, int start, int end) {
		WhereParser p = new WhereParser(str, start, end);
		left = p.parseBoolExpr();  	// the cursor should be the right parenthesis of {1stExpr args..}
		p.incCurrent();				// skip the space between left and right;
		right = p.parseBoolExpr(); 	// the cursor should be the right parenthesis of {2ndExpr args..}
		return p.incCurrent(); 	// the cursor should be the right parenthesis of {AndExpr left right}
	}

}
