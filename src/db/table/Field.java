package db.table;

import org.apache.hadoop.io.WritableComparable;

import db.sql.PredicateOp;

public interface Field extends WritableComparable<Field> {
	FieldType getFieldType();
	boolean boolOp(Field f, PredicateOp op) throws UnsupportedOperation;
	public int parseFieldsFromString(String str, int start, int end);
}
