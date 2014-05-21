package db.table;

import org.apache.hadoop.io.WritableComparable;

public interface Field extends WritableComparable<Field> {
	FieldType getFieldType();
}
