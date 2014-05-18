package db.sql;

public class NotExpr implements BoolExpr {
	BoolExpr child;

	public BoolExpr getChild() {
		return child;
	}

	public void setChild(BoolExpr child) {
		this.child = child;
	}
}
