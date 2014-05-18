package db.plan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.sql.BoolExpr;
import db.sql.TableDotColumn;
import db.table.Schema;

public class PlanOptimizer {
	
	Map<String,Schema> tables = new HashMap<String, Schema>();
	List<TableDotColumn> results;
	List<String> from;
	BoolExpr where;
	
	
	
	public void init(List<TableDotColumn> results, List<String> from, BoolExpr where) {
		this.results = results;
		this.from = from;
		this.where = where;
	}
	
	void generatePlan() {
		
	}

}
