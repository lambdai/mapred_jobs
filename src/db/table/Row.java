package db.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

public class Row implements WritableComparable<Row> {
	private Field[] fields;
	private Schema schema;

	public static Row createBySchema(Schema schema) {
		Row ret = new Row();
		ret.schema = schema;
		List<ColumnDescriptor> columnDef = schema.getRecordDescriptor();
		int nCol = columnDef.size();
		Field[] fds = new Field[nCol];
		int i = 0;
		for (ColumnDescriptor cd : columnDef) {
			fds[i] = cd.getFieldType().createInstance();
			i++;
		}
		ret.setFields(fds);
		return ret;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		for (Field f : getFields()) {
			f.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		if (getFields() == null) {
			throw new UnsupportedOperation(
					"read row fields without setup schema");
		}
		for (Field f : getFields()) {
			f.readFields(in);
		}
	}

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

	public void setFields(Field[] fields) {
		this.fields = fields;
	}

	public Field[] getFields() {
		return fields;
	}

}
