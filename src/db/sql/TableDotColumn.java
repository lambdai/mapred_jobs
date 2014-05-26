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
	
	public String toString() {
		if(tableName != null) {
			return "{" + this.getClass().getSimpleName() + " " + tableName + "." + columnName + "}" ;
		} else {
			return "{" + this.getClass().getSimpleName() + " " + columnName + "}";
		}
	}
	

	public int parseFieldsFromString(String str, int start, int end) {
		int inext = str.indexOf("}", start);
		String func = str.substring(start, inext);
		String[] strs = func.split("\\.");
		if(strs.length == 1) {
			tableName = null;
			columnName = strs[0];
		} else {
			tableName = strs[0];
			columnName = strs[1];
		}
		return inext;
	}
	
	
	public static void main(String args[]) {
		System.out.println(new TableDotColumn().getClass().getSimpleName());
	}
}
