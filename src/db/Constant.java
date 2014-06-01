package db;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public interface Constant {

	final public String AGG_COLUMNS = "agg_columns";
	final public String COLUMN_ID = "column_id";
	final public String DEST_VALUE = "dest_value";
	
	//for schema
	public final String INPUT_TABLE_SCHEMA = "input_table_schema";	
	final public String OUTPUT_TABLE_SCHEMA = "output_table_schema";
	public final String AGG_RESULT_SCHEMA = "agg_result_schema";
	
	final public String TABLENAME_COLUMNS_SPLIT = ":";
	final public String COLUMN_SPLIT = ";";
	final public String COLUMN_NAME_TYPE_SPLIT = ",";
	
	//for natural join
	final public String JOIN_USING = "join_using";
	final public String LEFT_JOIN_SCHEMA = "left_join_schema";
	final public String RIGHT_JOIN_SCHEMA = "right_join_schema";
	final public String JOIN_RESULT_SCHEMA = "result_join_schema";
	
	
	
	final public Text EMPTY_TEXT= new Text("");
	public final String AND_EXPR = "AND";
	public final String OR_EXPR = "OR";
	public final String NOT_EXPR = "NOT";
	public final String BOOL_VALUE_EXPR = "BVAL";
	
	
	
	public static final byte IntTypeId = 1;
	public static final byte StringTypeId = 2;
	
	public static final String GTID = ">";
	public static final String GEID = ">=";

	public static final String LTID = "<";
	public static final String LEID = "<=";
	
	public static final String EQID = "=";
	public static final String NEID = "!=";
	
	public static final BytesWritable EMPTY_BYTESWRITABLE = new BytesWritable(); 
	
}
