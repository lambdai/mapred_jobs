package db.sql;


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

	
}
