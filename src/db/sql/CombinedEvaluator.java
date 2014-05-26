package db.sql;

import java.util.ArrayList;
import java.util.List;

public abstract class CombinedEvaluator implements Evaluator {
	
	private List<Evaluator> subEvalutors = new ArrayList<Evaluator>();

	public List<Evaluator> getSubEvalutors() {
		return subEvalutors;
	}

	public void pushEvalutor(Evaluator eval) {
		subEvalutors.add(eval);
	}
	
	@Override
	public abstract boolean evalutate();

}
