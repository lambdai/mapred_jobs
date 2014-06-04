package db.sql;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;

import db.Dive;
import db.syntax.DiveBaseListener;
import db.syntax.DiveParser;
import db.syntax.DiveParser.Natural_join_clauseContext;
import db.syntax.DiveParser.Select_coreContext;
import db.table.AggFuncColumnDescriptor;
import db.table.ColumnDescriptor;
import db.table.FieldType;
import db.table.RowFormatException;
import db.table.Schema;
import db.table.SimpleColumnDescriptor;

public class DivePlanListener extends DiveBaseListener {
	
	public List<ColumnDescriptor> resultColumns = new ArrayList<ColumnDescriptor>();
	public List<ColumnDescriptor> groupbyColumns = new ArrayList<ColumnDescriptor>();
	public List<String> joinTables = new ArrayList<String>();
	public BoolExpr where;
	
	private BoolExprFactory bf = new BoolExprFactory();
	
	private PredicateExprFactory pf = new PredicateExprFactory();
	
	private Dive dive;
	
	
	private Schema schema = null;

    public Schema getSchema() { return schema; }

    @Override
    public void exitCreate_table_stmt(@NotNull DiveParser.Create_table_stmtContext ctx) {
        // Extract table name
    	schema = new Schema(ctx.table_name().getText());

        // Extract list of column definitions
        List<ColumnDescriptor> recordDescriptor = new ArrayList<ColumnDescriptor>();
        for (int i = 0; i < ctx.column_def().size(); i++) {
            String columnName = ctx.column_def(i).column_name().any_name().IDENTIFIER().getText();
            String typeName = ctx.column_def(i).type_name().name(0).any_name().IDENTIFIER().getText();
            // Convert type name to our own enum type
            FieldType fieldType;
            if (typeName.equals("int") || typeName.equals("smallint")) {
                fieldType = FieldType.IntType;
            }
            else if (typeName.equals("char") || typeName.equals("varchar")) {
                fieldType = FieldType.StringType;
            } else {
            	throw new RowFormatException(ctx.getText());
            }
            recordDescriptor.add(new SimpleColumnDescriptor(columnName, fieldType));
        }
        schema.setRecordDescriptor(recordDescriptor);
        dive.submitTable(schema);
    }
	
	public DivePlanListener(Dive dive) {
		this.dive = dive;
	}
	/*
	@Override
	public void enterTable_name(@NotNull DiveParser.Table_nameContext ctx){
		if(ctx.parent instanceof Select_coreContext) {
			tables.add(ctx.getText());
		} else if (ctx.parent instanceof Natural_join_clauseContext) {
			tables.add(ctx.getText());
		}
		System.out.println(ctx.getText());
	}
	*/
	public void initSelect() {
		resultColumns = new ArrayList<ColumnDescriptor>();
		groupbyColumns = new ArrayList<ColumnDescriptor>();
		joinTables = new ArrayList<String>();
		where = null;
		bf = new BoolExprFactory();
		pf = new PredicateExprFactory();
	}
	
	public void enterSelect_core(@NotNull DiveParser.Select_coreContext ctx) {
		initSelect();
	}	
	@Override
	public void exitSelect_core(@NotNull DiveParser.Select_coreContext ctx) {
		where = bf.stack.pop();
		try {
			dive.createSelectJob(resultColumns, groupbyColumns, joinTables, where);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public void exitTable_name(@NotNull DiveParser.Table_nameContext ctx){
		if(ctx.parent instanceof Select_coreContext) {
			joinTables.add(ctx.getText());
		} else if (ctx.parent instanceof Natural_join_clauseContext) {
			joinTables.add(ctx.getText());
		}
		//System.out.println(ctx.getText());
	}

	@Override
	public void exitBoolLeaf(@NotNull DiveParser.BoolLeafContext ctx) {
		bf.stack.push(pf.generatePredicate());
	}
	
	@Override
	public void enterPredicate_op(DiveParser.Predicate_opContext ctx) {
		pf.accept(PredicateOp.getByString(ctx.getText()));
	}
	
	@Override
	public void enterFvalLit(DiveParser.FvalLitContext ctx) {
		pf.accept(FieldOperand.create(ctx.getText()));
	}
	
	@Override
	public void enterFvalCol(DiveParser.FvalColContext ctx) {
		TableDotColumn tdc = new TableDotColumn();
		int size = ctx.children.size();
		tdc.setColumnName(ctx.children.get(size-1).getText());
		if(ctx.children.size() == 3) {
			tdc.setTableName(ctx.children.get(0).getChild(0).getText());
		}
		pf.accept(tdc);
	}
	
	
	
	@Override
	public void exitBoolAnd(@NotNull DiveParser.BoolAndContext ctx) {
		AndExpr e = new AndExpr();
		e.setRight(bf.stack.pop());
		e.setLeft(bf.stack.pop());
		bf.stack.push(e);
	}
	
	@Override
	public void exitBoolOr(@NotNull DiveParser.BoolOrContext ctx) {
		OrExpr e = new OrExpr();
		e.setRight(bf.stack.pop());
		e.setLeft(bf.stack.pop());
		bf.stack.push(e);
	}
	
	@Override
	public void exitBoolParenthesis(@NotNull DiveParser.BoolParenthesisContext ctx) {
		//donothing
	}
	
	@Override
	public void exitBoolNot(@NotNull DiveParser.BoolNotContext ctx) {
		NotExpr e = new NotExpr();
		e.setChild(bf.stack.pop());
		bf.stack.push(e);
	}
	
	public void exitColumnName(@NotNull DiveParser.ColumnNameContext ctx) {
		String colName = ctx.children.get(0).getText();
		resultColumns.add(new SimpleColumnDescriptor(colName, FieldType.IntType));
	}
	
	public void exitGbcolumn(@NotNull DiveParser.GbcolumnContext ctx) {
		String tcName = ctx.getText();
		String[] tc = tcName.split("\\.");
		String colName = tc[tc.length-1];
		groupbyColumns.add(new SimpleColumnDescriptor(colName, FieldType.IntType));
	}
	
	public void exitAggregationOperation(@NotNull DiveParser.AggregationOperationContext ctx) {
		AggFuncColumnDescriptor fcd = new AggFuncColumnDescriptor();
		fcd.setColumnName(ctx.getText().replaceAll(" \\t", ""));
		resultColumns.add(fcd);
	}
	
	
	
}
