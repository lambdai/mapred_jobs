package db;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import db.table.Schema;

public class JoinExecuter extends Configured implements Tool {

	private Schema leftSchema;
	private Schema rightSchema;
	private Schema outputSchema;
	
	
	@Override
	public int run(String[] args) throws Exception {
		
		return 0;
	}

}
