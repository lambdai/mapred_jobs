package db.sql;

import java.util.List;

import db.Constant;
import db.table.ColumnDescriptor;

public class SelectJob {
	
	public List<ColumnDescriptor> resultColumns;
	public List<ColumnDescriptor> groupbyColumns;
	public List<String> joinTables;
	public BoolExpr where;
	
	public SelectJob(List<ColumnDescriptor> resultColumns,
			List<ColumnDescriptor> groupbyColumns, List<String> tables,
			BoolExpr where) {
		super();
		this.resultColumns = resultColumns;
		this.groupbyColumns = groupbyColumns;
		this.joinTables = tables;
		this.where = where;
	}

	public void run() {
		dump();
	}
	
	public void dump() {
		dumpResultColumns();
		dumpJoinTables();
		dumpWhere();
		dumpGroupbyColumns();
	}

	private void dumpGroupbyColumns() {
		System.out.println("GROUPBY:" + groupbyColumns.size());
		for(ColumnDescriptor cd : groupbyColumns) {
			System.out.print(cd.toString());
			System.out.print(Constant.COLUMN_SPLIT);
		}
		System.out.println();		
	}

	private void dumpWhere() {
		System.out.println("WHERE:");
		System.out.println(where.toString());
	}

	private void dumpJoinTables() {
		System.out.println("FROM:" + joinTables.size());
		for(String table : joinTables) {
			System.out.print(table);
			System.out.print(Constant.COLUMN_SPLIT);
		}
		System.out.println();
	}

	private void dumpResultColumns() {
		System.out.println("SELECT:" + resultColumns.size());
		for(ColumnDescriptor cd : resultColumns) {
			System.out.print(cd.toString());
			System.out.print(Constant.COLUMN_SPLIT);
		}
		System.out.println();
	}
	
	
}
