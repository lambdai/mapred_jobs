package db.table;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

public class TestSchemaUtils extends TestCase {

	@Test
	public void testColumnLeft() {
		Assert.assertArrayEquals(SchemaUtils.columnLeft(new int[]{3,2,5}, 8), new int[]{0,1,4,6,7});
	}
	
	public void testParseColumns() {
		List<String> cols = new ArrayList<String>();
		cols.add("a");
		cols.add("b");
		cols.add("c");
		String intern = SchemaUtils.dumpColumns(cols);
		System.out.println(intern);
		List<String> list = SchemaUtils.parseColumns(intern);
		assertEquals(list, cols);	
	}

}
