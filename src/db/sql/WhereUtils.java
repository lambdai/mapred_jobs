package db.sql;

import db.table.RowFormatException;

public class WhereUtils {
	public static void main(String args[]) {
		//Gson can not deserialize interface from string 
		/*
		PredicateOperand o1 = new TableDotColumn("t1", "c1");
		PredicateOperand o2 = new TableDotColumn("t2", "c2");
		BoolValue bv = new BoolValue(o1,o2,PredicateOp.EQ);
		AndExpr expr = new AndExpr();
		expr.left = bv;
		expr.right = bv;
		Gson gson = new Gson();
		String json = gson.toJson(expr);
		
		Gson fgson = new Gson();
		BoolExpr dest = fgson.fromJson(json, AndExpr.class);
		System.out.println(dest);
		*/
	}
	
	
	
}
