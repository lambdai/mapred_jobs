package db.table;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

public class JoinRowFactory {
	IntField markField = new IntField(-1);
	DataInput in;

	// convert bytes stream to field
	public IntField readOneFieldFromBytes(BytesWritable bytes) throws IOException {
		in = new DataInputStream(new ByteArrayInputStream(bytes.getBytes()));
		markField.readFields(in);
		return markField;
	}
	
	public void readRemaining(Row row) throws IOException {
		row.readFields(in);
	}

}
