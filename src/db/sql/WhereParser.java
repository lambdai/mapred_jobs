package db.sql;

import db.Constant;
import db.table.RowFormatException;

public class WhereParser {

	String str;
	int start, end, icurrent;

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
		assert (str.charAt(icurrent) == '{');
		icurrent++;
		inext = str.indexOf(" ", icurrent);
		//TODO: 2nd argument is length or offset
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

	public BoolValue parseBoolValue(String str, int start, int end) {
		return null;
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
		// TODO Auto-generated method stub
		throw new RowFormatException(str);
	}

	public PredicateOperand parsePredicateOpereand() {
		// TODO Auto-generated method stub
		throw new RowFormatException(str);
	}

}
