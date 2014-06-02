package db.table;

public interface ColumnDescriptor {
	String getColumnName();
	void setColumnName(String columnName);
	FieldType getInputFieldType();
	void setInputFieldType(FieldType fieldType);
	FieldType getOutputFieldType();
	int parseFieldsFromString(String str, int start, int end);
}
