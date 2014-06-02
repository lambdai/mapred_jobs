package db.table;


public class SimpleColumnDescriptor implements ColumnDescriptor {

	private String columnName;
	private FieldType fieldType;

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public FieldType getInputFieldType() {
		return fieldType;
	}

	public void setInputFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	public FieldType getOutputFieldType() {
		return fieldType;
	}

	// constructor
	public SimpleColumnDescriptor(String cName, FieldType fType) {
		columnName = cName;
		fieldType = fType;
	}

	@Override
	public int hashCode() {
		return columnName.hashCode() ^ fieldType.getTypeId();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof SimpleColumnDescriptor)) {
			return false;
		}
		SimpleColumnDescriptor yacd = (SimpleColumnDescriptor) obj;
		return columnName.equals(yacd.getColumnName())
				&& getInputFieldType().equals(yacd.getInputFieldType())
				&& getOutputFieldType().equals(yacd.getOutputFieldType());
	}

	public String toString() {
		return String.format("{%s %s %d}", getClass().getSimpleName(),
				columnName, fieldType.getTypeId());
	}

	public int parseFieldsFromString(String str, int start, int end) {
		int ret = str.indexOf("}", start);
		int ispace = str.indexOf(" ", start);
		columnName = str.substring(start, ispace);
		fieldType = FieldType.getFieldTypeById(Byte.parseByte(str.substring(
				ispace + 1, ret)));
		return ret;
	}

}
