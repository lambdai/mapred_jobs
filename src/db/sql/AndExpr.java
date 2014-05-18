package db.sql;

public class AndExpr implements BoolExpr {
	BoolExpr left;
	BoolExpr right;

	public BoolExpr getLeft() {
		return left;
	}

	public void setLeft(BoolExpr left) {
		this.left = left;
	}

	public BoolExpr getRight() {
		return right;
	}

	public void setRight(BoolExpr right) {
		this.right = right;
	}

}
