package db.table;

import junit.framework.TestCase;

import org.junit.Test;

public class TestColumnDescriptor extends TestCase {
	
	ColumnDescriptor cd1 = new ColumnDescriptor("col1", FieldType.IntType);
	
	@Test
	public void testSerialization() {
		assertEquals(cd1.toString(), "col1,1;" );
	}
	
	public void testDeserialization() {
		ColumnDescriptor cd0 = ColumnDescriptor.create("col0,1");
		assertNotNull(cd0);
		assertEquals(cd0.getColumnName(), "col0");
		assertEquals(cd0.getFieldType(), FieldType.IntType);		
	}
}
