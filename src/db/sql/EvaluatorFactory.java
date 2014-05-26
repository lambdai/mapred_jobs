package db.sql;

public abstract class EvaluatorFactory {
	public abstract Evaluator create(BoolValue boolValue);	
}
