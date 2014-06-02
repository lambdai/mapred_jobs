package db.table;

import java.util.ArrayList;
import java.util.List;

public class ColumnDescUtils {
	
	public static ColumnDescriptor createCD(String s) {
		int iend = s.length() -1;
		assert(s.startsWith("{"));
		int iSplit = s.indexOf(" ");
		String type = s.substring(1, iSplit);
		ColumnDescriptor ret = null;
		if(type.equals(SimpleColumnDescriptor.class.getSimpleName())) {
			ret = new SimpleColumnDescriptor(null, null);
		} else if(type.equals(AggFuncColumnDescriptor.class.getSimpleName())) {
			ret = new AggFuncColumnDescriptor();
		}
		if (ret != null) {
			ret.parseFieldsFromString(s, iSplit+1, iend);
			return ret;
		}
		throw new RowFormatException(s);
	}
	
	public static List<AggFuncColumnDescriptor> filterAggerationCD (List<ColumnDescriptor> cds) {
		List<AggFuncColumnDescriptor> ret = new ArrayList<AggFuncColumnDescriptor>();
		for(ColumnDescriptor cd : cds) {
			if(cd instanceof AggFuncColumnDescriptor) {
				ret.add((AggFuncColumnDescriptor)cd);
			}
		}
		return ret;
	}
	
}
