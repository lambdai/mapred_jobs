package db.sql;

public interface EvaluatorFactory {
	Evaluator create(BoolValue boolValue);	
}
