package db.sql;

public class TableDotColumn implements PredicateOperand {

	String tableName;
	String columnName;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public TableDotColumn() {
	}

	public TableDotColumn(String table, String column) {
		this.tableName = table;
		this.columnName = column;
	}
	
}
