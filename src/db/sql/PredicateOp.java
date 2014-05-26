package db.sql;

import db.Constant;

public enum PredicateOp {

	LT {
		public String getStringId() {
			return Constant.LTID;
		}
	},
	LE {
		public String getStringId() {
			return Constant.LEID;
		}
	},
	GT {
		public String getStringId() {
			return Constant.GTID;
		}
	},
	GE {
		public String getStringId() {
			return Constant.GEID;
		}
	},
	EQ {
		public String getStringId() {
			return Constant.EQID;
		}
	},
	NEQ {
		public String getStringId() {
			return Constant.NEID;
		}
	};
	public abstract String getStringId();

	public static PredicateOp getByString(String str) {
		for (PredicateOp op : values()) {
			if (op.getStringId().equals(str)) {
				return op;
			}
		}
		throw new UnsupportedOperationException(String.format(
				"Wrong PredicateOp: %s", str));
	}
	
	public String toString() {
		return getStringId();
	}
	
	
}
