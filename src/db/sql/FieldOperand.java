package db.sql;

import db.table.Field;
import db.table.IntField;

public class FieldOperand implements PredicateOperand, FieldReadable {
	Field f;
	
	
	private FieldOperand(String str) {
		f = new IntField(Integer.parseInt(str));
	}


	public static FieldOperand create(String str) {
		FieldOperand ret = new FieldOperand(str);
		return ret;
	}


	public Field getF() {
		return f;
	}


	public void setF(Field f) {
		this.f = f;
	}
	
	public String toString() {
		return "{" + f.toString() + "}";
	}
	
	
}
