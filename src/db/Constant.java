package db;

import org.apache.hadoop.io.Text;

public interface Constant {

	final public String AGG_COLUMN_ID = "agg_column_id";
	final public String COLUMN_ID = "column_id";
	final public String DEST_VALUE = "dest_value";
	final public Text EMPTY_TEXT= new Text("");
	
	public static final byte IntTypeId = 1;
	public static final byte StringTypeId = 2;
}
