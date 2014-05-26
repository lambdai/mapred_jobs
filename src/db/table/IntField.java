package db.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

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
		if( tid != otid) {
			return tid - otid;
		}
		return value - ((IntField)o).value;
	}

	@Override
	public FieldType getFieldType() {
		return FieldType.IntType;
	}
	
	public int hashCode() {
		return value;
	}

	public boolean equals(Object o) {
		if(this == o) {
			return true;
		}
		if(o == null || !(o instanceof IntField)) {
			return false;
		}
		return value == ((IntField)o).value;
	}
	
	public void setValue(int val){	// added by ZX, plz mod/rmv if needed
		this.value=val;
	}
}
