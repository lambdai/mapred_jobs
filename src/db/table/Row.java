package db.table;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparable;

public class Row /*implements WritableComparable<Row>*/ {
	private Field[] fields;
	private Schema schema;

	public static final Field fieldMarkLeft = new IntField(1); 

	public static final Field fieldMarkRight = new IntField(2);

	
	public static Row createEmptyRow() {
		return new Row();
	}
	
	public static Row createBySchema(Schema schema) {
		Row ret = new Row();
		ret.initSchema(schema);
		return ret;
	}
	
	public void initSchema(Schema schema) {
		this.schema = schema;
		List<ColumnDescriptor> columnDef = schema.getRecordDescriptor();
		int nCol = columnDef.size();
		Field[] fds = new Field[nCol];
		int i = 0;
		for (ColumnDescriptor cd : columnDef) {
			fds[i] = cd.getOutputFieldType().createInstance();
			i++;
		}
		setFields(fds);
	}

	
	public void writeToBytesWithLeftMark(BytesWritable bytes, int[] iColumns) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		fieldMarkLeft.write(out);
		for(int i: iColumns) {
			fields[i].write(out);
		}
		out.flush();
		byte[] barray = bout.toByteArray();
		bytes.set(barray, 0, barray.length);
	}

	public void writeToBytesWithRightMark(BytesWritable bytes, int[] iColumns) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		fieldMarkRight.write(out);
		for(int i: iColumns) {
			fields[i].write(out);
		}
		out.flush();
		byte[] barray = bout.toByteArray();
		bytes.set(barray, 0, barray.length);
	}
	
	public void writeToBytes(BytesWritable bytes, int[] iColumns) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		for(int i: iColumns) {
			fields[i].write(out);
		}
		out.flush();
		byte[] barray = bout.toByteArray();
	    // Create a BytesWritable using the byte array as the initial value and length as the length.
		bytes.set(barray, 0, barray.length);
	}

	public void writeToBytes(BytesWritable bytes) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bout);
		write(out);
		out.flush();
		byte[] barray = bout.toByteArray();
		bytes.set(barray, 0, barray.length);
	}
		
	public void readFieldsFromBytes(BytesWritable bytes) throws IOException {
		// convert paramater into the byte stream
		DataInput bin = new DataInputStream(new ByteArrayInputStream(bytes.getBytes()));
		readFields(bin); // read in these byte steam
	}
	
	public void write(DataOutput out) throws IOException {
		for (Field f : getFields()) {
			f.write(out);
		}
	}
	
	
	public void readFields(DataInput in) throws IOException {
		if (getFields() == null) {
			throw new UnsupportedOperation(
					"read row fields without setup schema");
		}
		for (Field f : getFields()) {
			f.readFields(in);
		}
	}

	/*
	@Override
	//TODO: do not compare by length first
	public int compareTo(Row other) {
		if(this == other) {
			return 0;
		}
		Field[] f1 = getFields();
		Field[] f2 = other.getFields();
		if(f1.length < f2.length) {
			return -1;
		} else if(f1.length > f2.length) {
			return 1;
		}
		int ret;
		for(int i = 0; i < f1.length; i++) {
			ret = f1[i].compareTo(f2[i]);
			if (ret != 0) {
				return ret;
			}
		}
		return 0;
	}

	public boolean equals(Object o) {
		if(this == o) {
			return true;
		}
		if(o == null || !(o instanceof Row)) {
			return false;
		}
		Row other = (Row) o;
		
		Field[] f1 = getFields();
		Field[] f2 = other.getFields();
		if (f1.length != f2.length) {
			return false;
		}
		int len = f1.length;
		for (int i = 0; i < len; i++) {
			if (!f1[i].equals(f2[i])) {
				return false;
			}
		}
		return true;
	}

	public int hashCode() {
		int ret = 0;
		Field[] fs = getFields();
		
		ret ^= fs.length;
		for(Field f : fs) {
			ret ^= f.hashCode();
		}
		return ret;
	}
*/
	public void setFields(Field[] fields) {
		this.fields = fields;
	}

	public Field[] getFields() {
		return fields;
	}

	public Schema getSchema() {
		return schema;
	}

	public void setSchema(Schema schema) {
		this.schema = schema;
	}
	
	public void setField(Field fd, int index){	//added by ZX, mod/rmv if needed
		fields[index]=fd;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for(Field f : fields) {
			sb.append(f.toString());
			sb.append(';');
		}
		sb.setCharAt(sb.length()-1, '}');
		return sb.toString();
	}
}
