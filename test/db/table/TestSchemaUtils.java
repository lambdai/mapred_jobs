package db.table;

import junit.framework.TestCase;

import org.junit.Assert;
import org.junit.Test;

public class TestSchemaUtils extends TestCase {

	@Test
	public void testColumnLeft() {
		Assert.assertArrayEquals(SchemaUtils.columnLeft(new int[]{3,2,5}, 8), new int[]{0,1,4,6,7});
	}

}
