package db;
import java.util.ArrayList;
import java.util.List;

import db.table.ColumnDescriptor;
import db.table.Row;
import db.table.Schema;

public class ProjectPipe {
	private int[] colIndexes;
	private Row inRow;
	private Row outRow;
	//private Schema inSchema;
	//private Schema outSchema;
	
	public ProjectPipe(Schema inSchema, Schema outSchema){
		//this.inSchema = inSchema;
		//this.outSchema = outSchema;
		List<ColumnDescriptor> list = outSchema.getRecordDescriptor();
		List<String> nameList = new ArrayList<String>();
		for(ColumnDescriptor cd: list) {
			nameList.add(cd.getColumnName());
		}
		colIndexes = Schema.columnIndexes(inSchema, nameList);
		outRow = Row.createBySchema(outSchema);
	}
	
	public void write(Row row) {
		inRow = row;
	}

	public Row read() {
		for(int i=0;i<colIndexes.length;i++){
			outRow.setField(inRow.getFields()[colIndexes[i]], i);
		}		
		return outRow;
	}
	
}
