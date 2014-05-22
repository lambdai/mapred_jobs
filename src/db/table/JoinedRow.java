package db.table;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;

public class JoinedRow extends Row {

	private int nColForJoinKey;
	private int nLeftColumnRemained;
	private int nRightColumnRemained;

	private int writeCursor;

	public static JoinedRow createBySchema(Schema schema, Schema left,
			Schema right, String join_using_columns) {
		JoinedRow ret = new JoinedRow();
		ret.initSchema(schema);
		List<String> keyList = SchemaUtils.parseColumns(join_using_columns);
		ret.nColForJoinKey = keyList.size();

		// delete those repeating row?
		ret.nLeftColumnRemained = left.getRecordDescriptor().size()
				- ret.nColForJoinKey;
		ret.nRightColumnRemained = right.getRecordDescriptor().size()
				- ret.nColForJoinKey;
		return ret;
	}

	public void initByEquiColumns(BytesWritable key) throws IOException {
		Field[] fds = getFields();
		DataInput bin = new DataInputStream(new ByteArrayInputStream(
				key.getBytes()));

		// read the byte stream into field
		for (int i = 0; i < nColForJoinKey; i++) {
			fds[i].readFields(bin);
		}
		writeCursor = nColForJoinKey;
	}

	public void setCursorOnRight() {
		writeCursor = nLeftColumnRemained + nColForJoinKey;
	}

	public void setCursorOnLeft() {
		writeCursor = nColForJoinKey;
	}

	public void setCursorOnKey() {
		writeCursor = 0;
	}

	public void push(Row other) {
		Field[] otherFields = other.getFields();
		int len = otherFields.length;
		Field[] fields = getFields();
		for (int i = 0; i < len; i++) {
			fields[writeCursor++] = otherFields[i];
		}
	}

}
