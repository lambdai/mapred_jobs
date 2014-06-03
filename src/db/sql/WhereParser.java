package db.sql;

import db.Constant;
import db.table.Field;
import db.table.IntField;
import db.table.RowFormatException;
import db.table.StringField;

public class WhereParser {

	String str;
	int start, end, icurrent;
	
	public WhereParser(String str) {
		this(str, 0, str.length()-1);
	}

	public WhereParser(String str, int start, int end) {
		this.str = str;
		this.start = start;
		this.end = end;
		icurrent = start;
	}
	
	public int getCurrent() {
		return icurrent;
	}
	
	public int incCurrent() {
		return ++icurrent;
	}

	public BoolExpr parseBoolExpr() {
		int inext;
//		assert (str.charAt(icurrent) == '{');
//		System.out.println(str.substring(icurrent,72));
		icurrent++;
		inext = str.indexOf(" ", icurrent);
		//DONE: 2nd argument is offset from the beginning of the original string(excluded)
		String func = str.substring(icurrent, inext);
		BoolExpr ret = null;
		if (func.equals(Constant.AND_EXPR)) {
			ret = new AndExpr();
		} else if (func.equals(Constant.OR_EXPR)) {
			ret = new OrExpr();
		} else if (func.equals(Constant.NOT_EXPR)) {
			ret = new NotExpr();
		} else if (func.equals(Constant.BOOL_VALUE_EXPR)) {
			ret = new BoolValue();
		}
		if(ret != null) {
			icurrent = ret.parseFieldsFromString(str, inext+1, end) + 1;	//inext is the offset following the SPACE 
			return ret;
		}
		throw new RowFormatException(str);
	}


	public int findMatchParenthesis(int start) {
		int nOpen = 0;
		int len = str.length();
		int i = start;
		while (i < len) {
			char current = str.charAt(i);
			if (current == '{') {
				nOpen++;
			} else if (current == '}') {
				nOpen--;
				if (nOpen == 0) {
					return i;
				}
			}
		}
		throw new RowFormatException(str);
	}

	public PredicateOp parsePredicateOp() {
		int inext = str.indexOf(" ", icurrent);
		String opstr = str.substring(icurrent, inext);
		PredicateOp op = PredicateOp.getByString(opstr);
		icurrent = inext;
		return op;
	}

	public PredicateOperand parsePredicateOpereand() {
		int inext;
		assert (str.charAt(icurrent) == '{');
		icurrent++;
		inext = str.indexOf(" ", icurrent);
		String func = str.substring(icurrent, inext);
		PredicateOperand ret = null;
		if (func.equals(TableDotColumn.class.getSimpleName())) {
			ret = new TableDotColumn();
		} else if (func.equals(FieldOperand.class.getSimpleName())) {
			ret = FieldOperand.create("0");
		}
		if(ret != null) {
			icurrent = ret.parseFieldsFromString(str, inext+1, end) + 1;	//inext is the offset following the SPACE 
			return ret;
		}
		throw new RowFormatException(str);
	}

	public Field parseField() {
		int inext;
		assert (str.charAt(icurrent) == '{');
		icurrent++;
		inext = str.indexOf(" ", icurrent);
		String func = str.substring(icurrent, inext);
		Field ret = null;
		if (func.equals(IntField.class.getSimpleName())) {
			ret = new IntField(0);
		} else if (func.equals(FieldOperand.class.getSimpleName())) {
			ret = new StringField("");
		}
		if(ret != null) {
			icurrent = ret.parseFieldsFromString(str, inext+1, end) + 1;	//inext is the offset following the SPACE 
			return ret;
		}
		throw new RowFormatException(str);
	}

}
