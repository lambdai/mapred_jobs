package db.table;

import db.sql.aggregation.AggregationFunction;
import db.sql.aggregation.AggregationUtils;

public class AggFuncColumnDescriptor extends ColumnDescriptor {

	private AggregationFunction func;
	
	public AggFuncColumnDescriptor(String cName, FieldType fType) {
		super(cName, fType);
	}
	
	private AggFuncColumnDescriptor() {
		super(null,null);
	}
	
	public void setFunction(AggregationFunction func) {
		this.func = func;
	}
	
	public String toString() {
		return func.getFunctionName() + "(" + super.getColumnName() + ")";
	}

	public int parseFieldsFromString(String str, int start, int end) {
		int leftParenthesis = str.indexOf("(", start);
		int rightParenthesis = str.indexOf(")", leftParenthesis);
		String fName = str.substring(start, leftParenthesis);
		
		AggregationFunction tmpFunc = AggregationUtils.createFunctionInstance(fName);
		setFunction(tmpFunc);
		super.setColumnName(str.substring(leftParenthesis+1, rightParenthesis));
		return rightParenthesis;
	}
	
}
