package db.table;

import junit.framework.TestCase;
import db.sql.AndExpr;
import db.sql.BoolExpr;
import db.sql.BoolValue;
import db.sql.FieldOperand;
import db.sql.NotExpr;
import db.sql.OrExpr;
import db.sql.PredicateOp;
import db.sql.TableDotColumn;
import db.sql.WhereParser;

public class TestWhereParser extends TestCase {

	String tName1 = "t1";
	String cName1 = "col1";
	
	BoolExpr agt1;
	BoolExpr exprNot;
	BoolExpr exprAnd;
	BoolExpr exprOr;
	
	
	public void setUp() {
		agt1 = new BoolValue(new TableDotColumn(tName1, cName1), FieldOperand.create("1"), PredicateOp.EQ);
		exprNot = new NotExpr(agt1);
		BoolExpr bgtc = new BoolValue(new TableDotColumn("t2", "col2"), new TableDotColumn(null, "col3"), PredicateOp.GT);
		AndExpr and = new AndExpr();
		and.setLeft(agt1); and.setRight(bgtc);
		exprAnd = and;
		OrExpr or = new OrExpr();
		or.setLeft(and); or.setRight(exprNot);
		exprOr = or;
	}
	
	public void testSimple() {
		String str = agt1.toString();
		System.out.println(str);
		
		BoolExpr yagt1 = new WhereParser(str, 0, str.length()-1).parseBoolExpr();
		String yastr = yagt1.toString();
		
		System.out.println(yastr);
		assertEquals(yastr, str);
	}
	
	public void testNot() {
		
		String str = exprNot.toString();
		System.out.println(str);
		
		BoolExpr yaExprNot = new WhereParser(str, 0, str.length()-1).parseBoolExpr();
		String yastr = yaExprNot.toString();
		
		System.out.println(yastr);
		assertEquals(yastr, str);
	}

	public void testAnd() {
		
		String str = exprAnd.toString();
		System.out.println(str);
		
		BoolExpr yaExpr = new WhereParser(str, 0, str.length()-1).parseBoolExpr();
		String yastr = yaExpr.toString();
		
		System.out.println(yastr);
		assertEquals(yastr, str);
	}

	public void testOr() {
		
		String str = exprOr.toString();
		System.out.println(str);
		
		BoolExpr yaExpr = new WhereParser(str, 0, str.length()-1).parseBoolExpr();
		String yastr = yaExpr.toString();
		
		System.out.println(yastr);
		assertEquals(yastr, str);
	}
		
	
}
