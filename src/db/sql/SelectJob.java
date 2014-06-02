package db.sql;

import java.util.List;

import org.apache.hadoop.util.ToolRunner;

import db.Constant;
import db.JoinExecuter;
import db.plan.TableManager;
import db.table.ColumnDescriptor;
import db.table.Schema;

public class SelectJob {

	public List<ColumnDescriptor> resultColumns;
	public List<ColumnDescriptor> groupbyColumns;
	public List<String> joinTables;
	public BoolExpr where;

	boolean whereDone = false;
	
	public SelectJob(List<ColumnDescriptor> resultColumns,
			List<ColumnDescriptor> groupbyColumns, List<String> tables,
			BoolExpr where) {
		super();
		this.resultColumns = resultColumns;
		this.groupbyColumns = groupbyColumns;
		this.joinTables = tables;
		this.where = where;
	}

	public void run() throws Exception {
		Schema joinedSchema = join();
		System.out.println(joinedSchema.toString());
	}

	private Schema join() throws Exception {
		Schema ret = TableManager.getSchema(joinTables.get(0));
		int tSize = joinTables.size();

		for (int i = 1; i < tSize; i++) {
			if (i != tSize - 1) {
				ret = doJoin(ret, TableManager.getSchema(joinTables.get(i)));
			} else {
				ret = doJoinWithReducerWhere(ret,
						TableManager.getSchema(joinTables.get(i)), where);
				whereDone = true;
			}
		}
		return ret;
	}

	private Schema doJoinWithReducerWhere(Schema left, Schema right, BoolExpr w) throws Exception {
		Schema ret = TableManager.createTempTable();
		Schema joined = Schema.natualJoin(left, right);
		ret.setRecordDescriptor(joined.getRecordDescriptor());
		JoinExecuter executor = new JoinExecuter();
		executor.setLeftSchema(left);
		executor.setRightSchema(right);
		executor.setOutputSchema(ret);
		executor.setUsingColumns(Schema.columnNames(Schema.equiCols(left, right)));
		ToolRunner.run(executor, null);
		return ret;
	}

	private Schema doJoin(Schema left, Schema right) throws Exception {
		return doJoinWithReducerWhere(left, right, null);
	}

	public void dump() {
		dumpResultColumns();
		dumpJoinTables();
		dumpWhere();
		dumpGroupbyColumns();
	}

	private void dumpGroupbyColumns() {
		System.out.println("GROUPBY:" + groupbyColumns.size());
		for (ColumnDescriptor cd : groupbyColumns) {
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
		for (String table : joinTables) {
			System.out.print(table);
			System.out.print(Constant.COLUMN_SPLIT);
		}
		System.out.println();
	}

	private void dumpResultColumns() {
		System.out.println("SELECT:" + resultColumns.size());
		for (ColumnDescriptor cd : resultColumns) {
			System.out.print(cd.toString());
			System.out.print(Constant.COLUMN_SPLIT);
		}
		System.out.println();
	}

}
