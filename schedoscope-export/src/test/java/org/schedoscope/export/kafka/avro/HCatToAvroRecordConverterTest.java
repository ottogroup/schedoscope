package org.schedoscope.export.kafka.avro;

import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;

public class HCatToAvroRecordConverterTest extends HiveUnitBaseTest {

    HCatToAvroRecordConverter converter;

    @Override
    @Before
    public void setUp() throws Exception {

        super.setUp();
    }

    @Test
    public void testMapConverter() throws Exception {

        setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql", "test_map");
        converter = new HCatToAvroRecordConverter();
        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            GenericRecord rec = HCatToAvroRecordConverter.convertRecord(record, hcatInputSchema, "MyRecord");
        }
    }

    @Test
    public void testArrayConverter() throws Exception {

        setUpHiveServer("src/test/resources/test_array_data.txt", "src/test/resources/test_array.hql", "test_array");
        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            HCatToAvroRecordConverter.convertRecord(record, hcatInputSchema, "MyRecord");
        }
    }

    @Test
    public void testStructConverter() throws Exception {

        setUpHiveServer("src/test/resources/test_struct_data.txt", "src/test/resources/test_struct.hql", "test_struct");
        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            GenericRecord rec = HCatToAvroRecordConverter.convertRecord(record, hcatInputSchema, "MyRecord");
        }
    }

    @Test
    public void testMapMapConverter() throws Exception {

        setUpHiveServer("src/test/resources/test_mapmap_data.txt", "src/test/resources/test_mapmap.hql", "test_mapmap");
        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            GenericRecord rec = HCatToAvroRecordConverter.convertRecord(record, hcatInputSchema, "MyRecord");
        }
    }

    @Test
    public void testStructStructConverter() throws Exception {

        setUpHiveServer("src/test/resources/test_structstruct_data.txt", "src/test/resources/test_structstruct.hql", "test_structstruct");
        Iterator<HCatRecord> it = hcatRecordReader.read();
        while (it.hasNext()) {
            HCatRecord record = it.next();
            GenericRecord rec = HCatToAvroRecordConverter.convertRecord(record, hcatInputSchema, "MyRecord");
        }
    }
}
