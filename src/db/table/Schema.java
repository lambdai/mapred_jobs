package db.table;

import java.util.ArrayList;
import java.util.List;

import db.Constant;

public class Schema {

	private String tableName;
	private List<ColumnDescriptor> recordDescriptor;

	public boolean isValid() {
		return true;
	}

	private Schema() {
	}

	public Schema(String tableName) {
		this.tableName = tableName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	// convert a schema into local ArrayList (recordDescriptor)
	public void parseAndSetRecordDescriptor(String desc) {
		String[] columnMetas = desc.split(Constant.COLUMN_SPLIT);
		recordDescriptor = new ArrayList<ColumnDescriptor>();
		for (String columnMeta : columnMetas) {
			recordDescriptor.add(ColumnDescUtils.createCD(columnMeta));
		}
	}

	public String dumpRecordDescriptor() {
		StringBuilder sb = new StringBuilder();
		for (ColumnDescriptor cd : recordDescriptor) {
			sb.append(cd.toString());
			sb.append(Constant.COLUMN_SPLIT);
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
		return tableName + Constant.TABLENAME_COLUMNS_SPLIT
				+ dumpRecordDescriptor();
	}

	public static Schema createSchema(String schemaString) {
		if(schemaString == null || schemaString.length() == 0) {
			return null;
		}
		Schema ret = new Schema();
		int splitOffset = schemaString
				.indexOf(Constant.TABLENAME_COLUMNS_SPLIT);
		ret.setTableName(schemaString.substring(0, splitOffset));
		ret.parseAndSetRecordDescriptor(schemaString.substring(splitOffset
				+ Constant.TABLENAME_COLUMNS_SPLIT.length(),
				schemaString.length()));
		return ret;
	}

	public static int[] columnIndexes(Schema schema, List<String> required) {
		int[] ret = new int[required.size()]; // group by key word
		List<ColumnDescriptor> rd = schema.recordDescriptor; // getRecordDescriptor()?
		int rdlen = rd.size(); // whole schema length
		// search the group by key word and store into ret array
		nextSearch: for (int i = 0; i < ret.length; i++) {
			String current = required.get(i);
			for (int j = 0; j < rdlen; j++) {
				if (rd.get(j).getColumnName().equals(current)) {
					ret[i] = j;
					i++;
					continue nextSearch;
				}
			}
			throw new RowFormatException("no such column: " + current);
		}

		return ret; // return the array with group by key words
	}

	public static List<ColumnDescriptor> equiCols(Schema schema1, Schema schema2) {
		List<ColumnDescriptor> equiCols = new ArrayList<ColumnDescriptor>();
		for (ColumnDescriptor cd : schema1.recordDescriptor) {
			if (schema2.recordDescriptor.contains(cd)) {
				equiCols.add(cd);
			}
		}
		return equiCols;
	}
	
	public static List<String> columnNames(List<ColumnDescriptor> list) {
		List<String> ret = new ArrayList<String>(list.size());
		for(ColumnDescriptor cd: list) {
			ret.add(cd.getColumnName());
		}
		return ret;
	}

	public Schema createSubSchema(int[] subset) {
		Schema ret = new Schema("tmp");
		List<ColumnDescriptor> cds = new ArrayList<ColumnDescriptor>(
				subset.length);
		for (int i = 0; i < subset.length; i++) {
			cds.add(recordDescriptor.get(subset[i]));
		}
		ret.setRecordDescriptor(cds);
		return ret;
	}

	public static Schema natualJoin(Schema schema1, Schema schema2) {
		return natualJoin(schema1, schema2, equiCols(schema1, schema2));
	}

	public static Schema natualJoin(Schema schema1, Schema schema2,
			List<ColumnDescriptor> on) throws UnsupportedOperationException {
		Schema ret = new Schema("natualjoin");
		if (!schema1.isValid() || !schema2.isValid()) {
			throw (new UnsupportedOperationException("invalid Schema"));
		}

		List<ColumnDescriptor> tmpList = new ArrayList<ColumnDescriptor>();
		tmpList.addAll(on);

		for (ColumnDescriptor cd1 : schema1.recordDescriptor) {
			if (!on.contains(cd1)) {
				tmpList.add(cd1);
			}
		}

		for (ColumnDescriptor cd2 : schema2.recordDescriptor) {
			if (!on.contains(cd2)) {
				tmpList.add(cd2);
			}
		}

		ret.setRecordDescriptor(tmpList);
		return ret;
	}
}
