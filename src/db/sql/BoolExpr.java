package db.sql;


public interface BoolExpr {
	Evaluator createEvaluator(EvaluatorFactory factory);
	int parseFieldsFromString(String str, int start, int end);
}
