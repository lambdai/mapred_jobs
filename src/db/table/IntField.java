package db.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import db.sql.PredicateOp;
import db.sql.WhereParser;

public class IntField implements Field {
	int value;

	public IntField(int val) {
		value = val;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		new IntWritable(value).write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		IntWritable intw = new IntWritable();
		intw.readFields(in);
		value = intw.get();
	}

	@Override
	public int compareTo(Field o) {
		byte tid = getFieldType().getTypeId();
		byte otid = o.getFieldType().getTypeId();
		if (tid != otid) {
			return tid - otid;
		}
		return value - ((IntField) o).value;
	}

	@Override
	public FieldType getFieldType() {
		return FieldType.IntType;
	}

	public int hashCode() {
		return value;
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof IntField)) {
			return false;
		}
		return value == ((IntField) o).value;
	}

	@Override
	public boolean boolOp(Field f, PredicateOp op) {
		if (f.getFieldType() != getFieldType()) {
			throw new UnsupportedOperation(String.format(
					"Wrong arithmatic: (%s %s %s)", op.toString(),
					this.toString(), f.toString()));
		}
		IntField other = (IntField) f;
		switch (op) {
		case EQ:
			return value == other.value;
		case NEQ:
			return value != other.value;
		case GE:
			return value >= other.value;
		case GT:
			return value > other.value;
		case LE:
			return value <= other.value;
		case LT:
			return value < other.value;
		
		default:
			throw new UnsupportedOperation(String.format(
					"Wrong arithmatic: (%s %s %s)", op.toString(),
					this.toString(), f.toString()));
		}
	}
	
	public String toString() {
		return "{" + this.getClass().getSimpleName() + " " + value + "}"; 
	}

	@Override
	public int parseFieldsFromString(String str, int start, int end) {
		int inext = str.indexOf("}", start);
		value = Integer.valueOf(str.substring(start, inext));
		return inext;
	}
	
	public void setValue(int val){	// added by ZX, plz mod/rmv if needed
		this.value=val;
	}
	
	public int getValue() {
		return value;
	}
}
