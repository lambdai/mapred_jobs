package db.table;


public class ColumnDescriptor {

	String columnName;
	FieldType fieldType;
	
	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public void setFieldType(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	public ColumnDescriptor(String cName, FieldType fType) {
		columnName = cName;
		fieldType = fType;
	}
	
	public boolean equals(Object obj) {
		if(obj == null || ! (obj instanceof ColumnDescriptor)) {
			return false;
		}
		ColumnDescriptor yacd = (ColumnDescriptor)obj;
		return columnName.equals(yacd.getColumnName()) && fieldType.equals(yacd.getFieldType()) ;		
	}
	
	public String toString() {
		return String.format("%s,%d;", columnName, fieldType.getTypeId());
	}
	
	
	public static ColumnDescriptor create(String s) {
		String[] tmp = s.split(",");
		ColumnDescriptor cd = new ColumnDescriptor(tmp[0], FieldType.getFieldTypeById(Byte.parseByte(tmp[1])));
		return cd;
	}
	
	

}
