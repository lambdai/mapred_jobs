package db.sql;


public class RowEvaluator implements Evaluator {

	private RowEvaluationClosure closure;

	private FieldReadable f1;
	private FieldReadable f2;
	private PredicateOp op;
	
	public void init() {
		
	}
	
	@Override
	public boolean evalutate() {
		return f1.getF().boolOp(f2.getF(), op);
	}
	
	public RowEvaluationClosure getClosure() {
		return closure;
	}

	public void setClosure(RowEvaluationClosure closure) {
		this.closure = closure;
	}

	public FieldReadable getF1() {
		return f1;
	}

	public void setF1(FieldReadable f1) {
		this.f1 = f1;
	}

	public FieldReadable getF2() {
		return f2;
	}

	public void setF2(FieldReadable f2) {
		this.f2 = f2;
	}

	public PredicateOp getOp() {
		return op;
	}

	public void setOp(PredicateOp op) {
		this.op = op;
	}


	
	
}
