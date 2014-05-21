package db.sql;


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

	

}
