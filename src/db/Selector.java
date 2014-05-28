package db;
import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class Selector {
	private int[] colIndexes;
	private Row outRow;
	private Schema inSchema;
	private Schema outSchema;
	
	public Selector(Schema schema, String columnNames){
		inSchema=schema;
		outSchema = new Schema("SELECT FILTER");
		outSchema.parseAndSetRecordDescriptor(columnNames);
		colIndexes = Schema.columnIndexes(inSchema, SchemaUtils.parseColumns(columnNames));
		outSchema = inSchema.createSubSchema(colIndexes);		
	}
	
	//set the fields in output row and spit the row out 
	public Row select(Row inRow){
		for(int i=0;i<colIndexes.length;i++){
			outRow.setField(inRow.getFields()[colIndexes[i]], i);
		}		
		return outRow;
	}
}
