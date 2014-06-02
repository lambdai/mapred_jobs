package db;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Reducer;

import db.sql.AggRowFactory;
import db.sql.RowAccumulator;
import db.table.AggFuncColumnDescriptor;
import db.table.ColumnDescUtils;
import db.table.ColumnDescriptor;
import db.table.Field;
import db.table.Row;
import db.table.Schema;
import db.table.SchemaUtils;

public class GroupByReducer extends
		Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
	Row outRow; // the row for reduce output

	BytesWritable outKey;
	BytesWritable outValue; // the key & value for output

	RowAccumulator acc;
	Schema reducerInKeySchema;
	Schema reducerInValueSchema;
	Schema aggResultSchema;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		String inputSchemaString = conf.get(Constant.INPUT_TABLE_SCHEMA); 
		String groupByKeyString = conf.get(Constant.AGG_COLUMNS);
		Schema inSchema = new Schema("groupByInput");
		inSchema.parseAndSetRecordDescriptor(inputSchemaString);
		int[] keyIndexes = Schema.columnIndexes(inSchema,
				SchemaUtils.parseColumns(groupByKeyString));
		int[] valueIndexes = SchemaUtils.columnLeft(keyIndexes, inSchema
				.getRecordDescriptor().size());
		reducerInKeySchema = inSchema.createSubSchema(keyIndexes);
		reducerInValueSchema = inSchema.createSubSchema(valueIndexes);

		String aggResultSchemaString = conf.get(Constant.AGG_RESULT_SCHEMA);
		aggResultSchema = new Schema("AggOutput");
		aggResultSchema.parseAndSetRecordDescriptor(aggResultSchemaString);
		String reducerOutputSchemaString = conf
				.get(Constant.OUTPUT_TABLE_SCHEMA);
		
		
		Schema reducerOutputSchema = new Schema("groupbyReducerOutput");
		reducerOutputSchema
				.parseAndSetRecordDescriptor(reducerOutputSchemaString);
		List<ColumnDescriptor> cds = reducerOutputSchema.getRecordDescriptor();
		List<AggFuncColumnDescriptor> requiredAggCDs = ColumnDescUtils
				.filterAggerationCD(cds);

		AggRowFactory factory = new AggRowFactory();
		factory.setRequiredCDs(requiredAggCDs);
		factory.setInputRowSchema(reducerInValueSchema);
		acc = factory.createAccumulator();

		outKey = Constant.EMPTY_BYTESWRITABLE;
		outValue = new BytesWritable();

	}

	@Override
	protected void reduce(BytesWritable key, Iterable<BytesWritable> values,
			Context context) throws IOException, InterruptedException {
		
		Row inValueRow = Row.createBySchema(reducerInValueSchema);
		
		acc.init();
		for(BytesWritable inValue : values) {
			inValueRow.readFieldsFromBytes(inValue);
			acc.accept(inValueRow);			
		}
		Row aggColumnRow = acc.submitRow();
		
		Row keysRow = Row.createBySchema(reducerInKeySchema);
		
		Row aggResultRow = Row.createBySchema(aggResultSchema);
		int nKeyColumn = keysRow.getFields().length;
		for(int i = 0; i < nKeyColumn; i++) {
			aggResultRow.setField(keysRow.getFields()[i], i);
		}
		Field[] aggResultColumn = aggColumnRow.getFields();
		for(int i = 0; i < aggResultColumn.length; i++) {
			aggResultRow.setField(keysRow.getFields()[i], i+nKeyColumn);
		}
		//TODO: filter some keys
		
		aggResultRow.writeToBytes(outValue);
		context.write(outKey, outValue); // output
	}

}
