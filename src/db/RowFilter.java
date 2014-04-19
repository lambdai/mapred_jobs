package db;

public interface RowFilter {
	boolean filtered(String []row);
}
