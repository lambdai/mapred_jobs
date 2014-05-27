package db.table;

import db.sql.aggregation.AggregationFunction;

public class AggFuncColumnDescriptor extends ColumnDescriptor {

	AggregationFunction func;
	public AggFuncColumnDescriptor(String cName, FieldType fType) {
		super(cName, fType);
	}
	
	public void setFunction(AggregationFunction func) {
		this.func = func;
	}
	
	public 

}
