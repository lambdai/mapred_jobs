package db.sql;

import java.util.List;

import db.sql.aggregation.AggregationFunction;
import db.table.AggFuncColumnDescriptor;
import db.table.ColumnDescriptor;
import db.table.RowFormatException;
import db.table.Schema;

public class AggRowFactory {

	private List<AggFuncColumnDescriptor> columns;
	private Schema onSchema;
	
	public void setRequiredCDs(List<AggFuncColumnDescriptor> requiredAggCDs) {
		columns = requiredAggCDs;
	}

	public void setInputRowSchema(Schema valueSchema) {
		onSchema = valueSchema;
	}

	public RowAccumulator createAccumulator() {
		
		int len = columns.size();
		AggregationFunction[] cds = new AggregationFunction[len];
		int[] offsets = new int[len];
		int i = 0;
		List<ColumnDescriptor> simpleCDList = onSchema.getRecordDescriptor();
		for(AggFuncColumnDescriptor aggCD: columns) {
			cds[i] = aggCD.getFunc();
			int j = 0;
			for(ColumnDescriptor cd : simpleCDList) {
				if(cd.getColumnName().equals(aggCD.onColumnName())) {
					offsets[i] = j;
					break;
				}
				j++;
			}
			if(j == len) {
				throw new RowFormatException(String.format("Wrong schema: %s\n while try to aggregate on %s\n", onSchema.toString(), aggCD.toString()));
			}
			i++;
		}
		RowAccumulator ret = new RowAccumulator(cds, offsets);
		return ret;
	}

}
