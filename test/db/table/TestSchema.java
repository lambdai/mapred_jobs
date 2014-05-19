package db.table;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

public class TestSchema extends TestCase {
	
	String t1 = "t1";
	String rd1Str = "name1,1;name2,2;";
	
	@Test
	public void testSerialization () {
		Schema s = new Schema(t1);
		s.parseAndSetRecordDescriptor(rd1Str);
		List<ColumnDescriptor> cds = s.getRecordDescriptor();
		assertEquals(cds.size(), 2);
		assertEquals(cds.get(1).columnName, "name2");
		assertEquals(cds.get(1).fieldType, FieldType.StringType);
	}
	
}
