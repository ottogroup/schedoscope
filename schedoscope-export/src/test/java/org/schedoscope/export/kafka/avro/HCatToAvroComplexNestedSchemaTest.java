package org.schedoscope.export.kafka.avro;

import static org.junit.Assert.assertNotNull;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;

public class HCatToAvroComplexNestedSchemaTest extends HiveUnitBaseTest {

	@Override
	@Before
	public void setUp() throws Exception {

		super.setUp();
		setUpHiveServerNoData("src/test/resources/test_complex_nested_structures.hql", "test_complex_nested_structures");
	}

	@Test
	public void testComplexSchemaConversion() throws Exception {

		Schema schema = HCatToAvroRecordConverter.convertSchema(hcatInputSchema, "test_complex_nested_structures");
		assertNotNull(schema);
	}
}
