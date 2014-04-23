package db.table;

import java.util.ArrayList;
import java.util.List;

public class Schema {
	
	private String tableName;
	private List<ColumnDescriptor> recordDescriptor;
	


	public boolean isValid() {
		return true;
	}
	
	public Schema (String tableName) {
		this.tableName = tableName;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	
	public void parseAndSetRecordDescriptor (String desc) {
		String[] columnMetas = desc.split(";");
		recordDescriptor = new ArrayList<ColumnDescriptor>();
		for(String columnMeta : columnMetas) {
			recordDescriptor.add(ColumnDescriptor.create(columnMeta));
		}
	}
	
	public String dumpRecordDescriptor () {
		StringBuilder sb = new StringBuilder();
		for(ColumnDescriptor cd: recordDescriptor) {
			sb.append(cd.toString());
		}
		return sb.toString();
	}
	
	public List<ColumnDescriptor> getRecordDescriptor() {
		return recordDescriptor;
	}

	public void setRecordDescriptor(List<ColumnDescriptor> recordDescriptor) {
		this.recordDescriptor = recordDescriptor;
	}
	
	public String toString() {
		return tableName + " " + dumpRecordDescriptor();
	}
	
	
	public static Schema natualJoin(Schema schema1, Schema schema2, List<ColumnDescriptor> on) throws UnsupportedOperationException {
		Schema ret = new Schema("natualjoin");
		if(!schema1.isValid() || schema2.isValid() ) {
			throw(new UnsupportedOperationException("invalid Schema"));
		}
		List<ColumnDescriptor> tmpList = new ArrayList<ColumnDescriptor>();
		tmpList.addAll(schema1.recordDescriptor);
		for(ColumnDescriptor cd2 : schema2.recordDescriptor) {
			if (!ret.recordDescriptor.contains(cd2)) {
				tmpList.add(cd2);
			} else {
				on.add(cd2);
			}
		}
		ret.setRecordDescriptor(tmpList);
		return ret;
	}
}
