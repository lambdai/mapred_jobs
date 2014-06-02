package db.table;

import junit.framework.TestCase;

import org.junit.Test;

public class TestColumnDescriptor extends TestCase {
	
	ColumnDescriptor cd1 = new SimpleColumnDescriptor("col1", FieldType.IntType);
	ColumnDescriptor cd2 = new AggFuncColumnDescriptor();
	
	public void setUp() {
		cd2.setColumnName("SUM(col2)");
		cd2.setInputFieldType(FieldType.StringType);
	}
	@Test
	public void testSerializationSimple() {
		assertEquals(cd1.toString(), "{SimpleColumnDescriptor col1 1}" );
	}
	
	public void testDeserializationSimple() {
		ColumnDescriptor cd0 = ColumnDescUtils.createCD("{SimpleColumnDescriptor col0 1}");
		assertNotNull(cd0);
		assertEquals(cd0.getColumnName(), "col0");
		assertEquals(cd0.getInputFieldType(), FieldType.IntType);
		assertEquals(cd0.getOutputFieldType(), FieldType.IntType);
	}
	
	@Test
	public void testSerializationComplex() {
		assertEquals(cd2.toString(), "{AggFuncColumnDescriptor SUM {SimpleColumnDescriptor col2 2}}" );
	}
	
	public void testDeserializationComplex() {
		ColumnDescriptor cd0 = ColumnDescUtils.createCD("{AggFuncColumnDescriptor SUM {SimpleColumnDescriptor col2 2}}");
		assertNotNull(cd0);
		assertEquals(cd0.getColumnName(), "SUM(col2)");
		assertEquals(cd0.getInputFieldType(), FieldType.StringType);
		assertEquals(cd0.getOutputFieldType(), FieldType.IntType);
	}
}
