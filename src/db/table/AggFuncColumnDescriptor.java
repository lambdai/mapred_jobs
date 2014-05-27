package db.table;

import db.sql.aggregation.AggregationFunction;
import db.sql.aggregation.AggregationUtils;

public class AggFuncColumnDescriptor implements ColumnDescriptor {

	private AggregationFunction func;
	private SimpleColumnDescriptor argColumn;

	
	public AggFuncColumnDescriptor() {
	}

	public AggregationFunction getFunc() {
		return func;
	}

	
	public void setFunction(AggregationFunction func) {
		this.func = func;
	}

	@Override
	public int hashCode() {
		return func.getFunctionName().hashCode() ^ argColumn.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof AggFuncColumnDescriptor)) {
			return false;
		}
		AggFuncColumnDescriptor yacd = (AggFuncColumnDescriptor) obj;
		return func.getFunctionName().equals(yacd.func.getFunctionName())
				&& argColumn.equals(yacd.argColumn);
	}

	public String toString() {
		return "{" + getClass().getSimpleName() + " " + func.getFunctionName()
				+ " " + argColumn.toString() + "}";
	}

	@Override
	public FieldType getOutputFieldType() {
		return func.outputFieldType();
	}

	@Override
	public String getColumnName() {
		return func.getFunctionName() + "(" + argColumn.getColumnName() + ")";
	}
	
	public String onColumnName() {
		return argColumn.getColumnName();
	}

	@Override
	public void setColumnName(String columnName) {
		int leftParenthesis = columnName.indexOf("(");
		int rightParenthesis = columnName.indexOf(")", leftParenthesis);
		String fName = columnName.substring(0, leftParenthesis);

		AggregationFunction tmpFunc = AggregationUtils
				.createFunctionInstance(fName);
		setFunction(tmpFunc);
		if (argColumn == null) {
			argColumn = new SimpleColumnDescriptor(null, FieldType.IntType);
		}
		argColumn.setColumnName(columnName.substring(leftParenthesis + 1,
				rightParenthesis));
	}

	@Override
	public FieldType getInputFieldType() {
		return argColumn.getInputFieldType();
	}

	@Override
	public void setInputFieldType(FieldType fieldType) {
		if (argColumn == null) {
			argColumn = new SimpleColumnDescriptor(null, fieldType);
		} else {
			argColumn.setInputFieldType(fieldType);
		}
	}

	public int parseFieldsFromString(String str, int start, int end) {
		System.out.println(str);
		int iSplit = str.indexOf(" ", start);
		setFunction(AggregationUtils.createFunctionInstance(str.substring(
				start, iSplit)));
		argColumn = new SimpleColumnDescriptor(null, null);
		iSplit = str.indexOf(" ", iSplit+1);
		return argColumn.parseFieldsFromString(str, iSplit + 1, end) + 1;
	}

}
