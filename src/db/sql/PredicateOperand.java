package db.sql;

public interface PredicateOperand {
	public int parseFieldsFromString(String str, int start, int end);
}
