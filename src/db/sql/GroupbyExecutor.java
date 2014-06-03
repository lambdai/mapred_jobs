package db.sql;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import db.Constant;
import db.DbPathFilter;
import db.DbPathUtils;
import db.GroupByMapper;
import db.GroupByReducer;
import db.table.ColumnDescriptor;
import db.table.Schema;

public class GroupbyExecutor extends Configured implements Tool {

	public static final Log LOG = LogFactory.getLog(GroupbyExecutor.class);

	private Schema originSchema;
	private List<ColumnDescriptor> groupbyColumns;
	private Schema reducerOutputSchema;
	private BoolExpr where;

	private String getGroupByString() {
		String ret = "";
		if (groupbyColumns == null || groupbyColumns.size() == 0) {
			return ret;
		}
		for (ColumnDescriptor cd : groupbyColumns) {
			ret += cd.getColumnName();
			ret += Constant.COLUMN_SPLIT;
		}
		return ret;
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		conf.set(Constant.INPUT_TABLE_SCHEMA, originSchema.toString());
		conf.set(Constant.AGG_COLUMNS, getGroupByString());
		conf.set(Constant.OUTPUT_TABLE_SCHEMA, reducerOutputSchema.toString());
		if (where != null) {
			conf.set(Constant.WHERE, where.toString());
		}

		Job job = new Job(conf, String.format("groupby %s", getGroupByString()));

		job.setJarByClass(getClass());

		FileSystem fs = FileSystem.get(conf);

		FileStatus[] files = fs.listStatus(
				DbPathUtils.tablePath(originSchema.getTableName()),
				new PathFilter() {
					@Override
					public boolean accept(Path path) {
						return !path.getName().equals("_SUCCESS")
								&& !path.getName().equals("_logs");
					}

				});

		job.setMapperClass(GroupByMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		for (FileStatus stat : files) {
			FileInputFormat.addInputPath(job, stat.getPath());
		}
		// SequenceFileInputFormat.addInputPath(job, files[0].getPath());
		// LOG.fatal(files[0].getPath());
		// SequenceFileInputFormat.setInputPathFilter(job, DbPathFilter.class);

		// job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setReducerClass(GroupByReducer.class);

		job.setNumReduceTasks(2);

		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job,
				DbPathUtils.tablePath(reducerOutputSchema));

		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public void setOriginSchema(Schema originSchema) {
		this.originSchema = originSchema;
	}

	public void setGroupbyColumns(List<ColumnDescriptor> groupbyColumns) {
		this.groupbyColumns = groupbyColumns;
	}

	public void setReducerOutputSchema(Schema reducerOutputSchema) {
		this.reducerOutputSchema = reducerOutputSchema;
	}

	public void setWhere(BoolExpr where) {
		this.where = where;
	}

}
