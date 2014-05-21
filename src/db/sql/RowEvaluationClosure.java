package db.sql;

import db.table.Row;
import db.table.Schema;

public class RowEvaluationClosure {
	
	Schema schema;
	Row row;
	
	public Schema getSchema() {
		return schema;
	}
	public void setSchema(Schema schema) {
		this.schema = schema;
	}
	public Row getRow() {
		return row;
	}
	public void setRow(Row row) {
		this.row = row;
	}

}
