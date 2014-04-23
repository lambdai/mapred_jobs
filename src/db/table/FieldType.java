package db.table;

import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import db.Constant;

public enum FieldType {

	IntType {

		@Override
		public Field parse(DataInput in) throws RowFormatException {
			IntWritable column = new IntWritable();
			try {
				column.readFields(in);
			} catch (IOException e) {
				throw new RowFormatException(e);
			}
			IntField field = new IntField(column.get());
			return field;
		}

		@Override
		public byte getTypeId() {
			return Constant.IntTypeId;
		}
	},
	StringType {
		@Override
		public Field parse(DataInput in) throws RowFormatException {
			Text column = new Text();
			try {
				column.readFields(in);
			} catch (IOException e) {
				throw new RowFormatException(e);
			}
			StringField field = new StringField(column.toString());
			return field;
		}

		@Override
		public byte getTypeId() {
			return Constant.StringTypeId;
		}

	};

	public abstract byte getTypeId();

	public abstract Field parse(DataInput in) throws RowFormatException;
	
	public static FieldType getFieldTypeById(byte fid) {
		for(FieldType f : values()) {
			if(f.getTypeId() == fid) {
				return f;
			}
		}
		System.err.printf("UnSupported Field Type Id: %d", fid);
		System.exit(1);
		return null;
	}

}
