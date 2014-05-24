package db.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import db.sql.PredicateOp;

public class StringField implements Field {
	String str;

	public StringField(String str) {
		this.str = str;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		new Text(str).write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text text = new Text();
		text.readFields(in);
		str = text.toString();

	}

	@Override
	public int compareTo(Field o) {
		byte tid = getFieldType().getTypeId();
		byte otid = o.getFieldType().getTypeId();
		if (tid != otid) {
			return tid - otid;
		}
		return str.compareTo(((StringField) o).str);
	}

	@Override
	public FieldType getFieldType() {
		return FieldType.StringType;
	}

	public int hashCode() {
		return str.hashCode();
	}

	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof StringField)) {
			return false;
		}
		return str == ((StringField) o).str;
	}

	@Override
	public boolean boolOp(Field f, PredicateOp op) throws UnsupportedOperation {
		throw new UnsupportedOperation(String.format(
				"Wrong arithmatic: (%s %s %s)", op.toString(),
				this.toString(), f.toString()));
	}
	
	public String toString() {
		return "{" + this.getClass().getCanonicalName() + " " + str + "}"; 
	}
}
