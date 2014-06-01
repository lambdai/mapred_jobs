package db.table;

import java.util.List;

import junit.framework.TestCase;

import org.junit.Test;

public class TestSchema extends TestCase {
	
	String t1 = "t1";
	String rd1Str = "{SimpleColumnDescriptor name1 1};{SimpleColumnDescriptor name2 2}";
	
	@Test
	public void testSerialization () {
		Schema s = new Schema(t1);
		s.parseAndSetRecordDescriptor(rd1Str);
		List<ColumnDescriptor> cds = s.getRecordDescriptor();
		assertEquals(cds.size(), 2);
		assertEquals(cds.get(1).getColumnName(), "name2");
		assertEquals(cds.get(1).getOutputFieldType(), FieldType.StringType);
		assertEquals(cds.get(1).getInputFieldType(), FieldType.StringType);
	}
	
	public void testWholeSchemaSchemaSerialization() {
		Schema s = new Schema(t1);
		s.parseAndSetRecordDescriptor(rd1Str);
		String wholeSchemaString = s.toString();
		Schema generated = Schema.createSchema(wholeSchemaString);
		assertEquals(s.toString(), generated.toString());
		assertEquals(s.getTableName(), generated.getTableName());
		assertEquals(s.getRecordDescriptor(), generated.getRecordDescriptor());
		
	}
	
}
