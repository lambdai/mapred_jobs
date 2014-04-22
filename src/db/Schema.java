package db;

public class Schema {
	public enum ColumnType {
		Integer,
		String
	}
	private String tableName;
	private String[] columnNames;
	private ColumnType[] columnTypes;
	
	public boolean isValid() {
		return tableName != null && columnTypes != null && columnNames != null && columnNames.length == columnTypes.length;
	}
	
	public Schema(String tableName, String[] columnNames, ColumnType[] columnTypes) {
		this.tableName = tableName;
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
	}
	
	public Schema(String tName, String[] cNames) {
		this(tName, cNames, new ColumnType[cNames.length]);
		for(int i = 0; i< columnTypes.length; i++) {
			columnTypes[i] = ColumnType.Integer;
		}		
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String[] getColumnNames() {
		return columnNames;
	}

	public void setColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}

	public ColumnType[] getColumnTypes() {
		return columnTypes;
	}

	public void setColumnTypes(ColumnType[] columnTypes) {
		this.columnTypes = columnTypes;
	}

	
}
