package db;

import java.io.IOException;
import java.util.List;

import org.antlr.v4.runtime.ANTLRFileStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import db.plan.TableManager;
import db.sql.BoolExpr;
import db.sql.DivePlanListener;
import db.sql.SelectJob;
import db.syntax.DiveLexer;
import db.syntax.DiveParser;
import db.table.ColumnDescriptor;
import db.table.Schema;

public class Dive {
	
	private SelectJob selectJob;
	
	public static void initTables() {
		Schema schema1 = new Schema("R");
		schema1.parseAndSetRecordDescriptor("{SimpleColumnDescriptor a 1};{SimpleColumnDescriptor b 1}");
		TableManager.putSchema(schema1);
		Schema schema2 = new Schema("S");
		schema2.parseAndSetRecordDescriptor("{SimpleColumnDescriptor b 1};{SimpleColumnDescriptor c 1}");
		Schema schema3 = new Schema("T");
		schema3.parseAndSetRecordDescriptor("{SimpleColumnDescriptor c 1};{SimpleColumnDescriptor d 1}");
		TableManager.putSchema(schema1);
		TableManager.putSchema(schema2);
		TableManager.putSchema(schema3);
	}
	
	public static void main(String args[]) throws IOException {
		
		initTables();
		
		DiveLexer lexer = new DiveLexer(new ANTLRFileStream("data/sql"));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		DiveParser parser = new DiveParser(tokens);
		ParseTree tree = parser.parse();
		ParseTreeWalker walker = new ParseTreeWalker();
		
		Dive dive = new Dive();
		
		DivePlanListener listener = new DivePlanListener(dive);
		
		walker.walk(listener, tree);
	}

	public void createSelectJob(List<ColumnDescriptor> resultColumns,
			List<ColumnDescriptor> groupbyColumns, List<String> joinTables,
			BoolExpr where) throws Exception {
		selectJob = new SelectJob(resultColumns, groupbyColumns, joinTables, where);
		selectJob.run();
	}

}
