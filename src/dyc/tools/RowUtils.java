package dyc.tools;

import db.table.Field;
import db.table.IntField;
import db.table.Row;

public class RowUtils {
	public static void printIntFieldRow(Row row) {

		for (Field field : row.getFields()) {
			IntField intf = (IntField) field;
			System.out.print(String.format("%10d", intf.getValue()));
		}
		System.out.println();
	}
}
