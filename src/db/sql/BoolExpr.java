package db.sql;


public interface BoolExpr {
	Evaluator createEvaluator(EvaluatorFactory factory);
}
