package db.sql;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.misc.NotNull;

import db.syntax.DiveBaseListener;
import db.syntax.DiveParser;
import db.syntax.DiveParser.Natural_join_clauseContext;
import db.syntax.DiveParser.Select_coreContext;

public class DivePlanListener extends DiveBaseListener {
	
	public List<String> tables = new ArrayList<String>();
	
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
	@Override
	public void exitTable_name(@NotNull DiveParser.Table_nameContext ctx){
		if(ctx.parent instanceof Select_coreContext) {
			tables.add(ctx.getText());
		} else if (ctx.parent instanceof Natural_join_clauseContext) {
			tables.add(ctx.getText());
		}
		//System.out.println(ctx.getText());
	}

}
