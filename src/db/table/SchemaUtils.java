package db.table;

import java.util.Arrays;
import java.util.List;

import db.Constant;

public class SchemaUtils {

	public static List<String> parseColumns(String str) {
		List<String> list = Arrays.asList(str.split(Constant.COLUMN_SPLIT));
		return list;
	}
	
	public static String dumpColumns(List<String> list) {
		StringBuilder sb = new StringBuilder();
		for(String str : list) {
			sb.append(str);
			sb.append(Constant.COLUMN_SPLIT);
		}
		return sb.toString();
	}
	
	public static int[] columnLeft(int[] has, int max) {
		int [] ret = new int[max-has.length]; // smart job!
		int offset = 0;
		nextPotentialColumnIndex:
		for(int i = 0; i < max; i++) {
			for(int j : has) {
				if(i == j) { // exist key word so skip
					continue nextPotentialColumnIndex;
				}
			}
			ret[offset++] = i; // store the left key word such as salary
		}
		return ret;
	}

}
