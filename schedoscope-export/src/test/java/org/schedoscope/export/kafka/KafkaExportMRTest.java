package org.schedoscope.export.kafka;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.schedoscope.export.HiveUnitBaseTest;
import org.schedoscope.export.kafka.avro.HCatToAvroRecordConverter;
import org.schedoscope.export.kafka.outputformat.KafkaOutputFormat;
import org.schedoscope.export.testsupport.EmbeddedKafkaCluster;

public class KafkaExportMRTest extends HiveUnitBaseTest {

    protected EmbeddedKafkaCluster kafka;

    protected TestingServer zkServer;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        setUpHiveServer("src/test/resources/test_map_data.txt", "src/test/resources/test_map.hql",
                "test_map");
        startKafkaServer();
    }

    @Override
    @After
    public void tearDown() throws Exception {

        stopKafkaServer();
        super.tearDown();
    }

    @Test
    public void testKafkaMapExport() throws Exception {

        Job job = Job.getInstance(conf);

        Schema schema = HCatToAvroRecordConverter.convertSchema(hcatInputSchema, "MyTable");
        AvroJob.setMapOutputValueSchema(job, schema);

        job.setMapperClass(KafkaExportMapper.class);
        job.setReducerClass(KafkaExportReducer.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(HCatInputFormat.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);

        job.setOutputKeyClass(String.class);
        job.setOutputValueClass(GenericRecord.class);

        assertTrue(job.waitForCompletion(true));
    }

    private void startKafkaServer() throws Exception {

        zkServer = new TestingServer(2181);
        zkServer.start();
        Thread.sleep(1000);

        ArrayList<Integer> ports = new ArrayList<Integer>();
        ports.add(9092);
        kafka = new EmbeddedKafkaCluster(zkServer.getConnectString(), new Properties(), ports);
        kafka.startup();
        Thread.sleep(2000);
    }

    private void stopKafkaServer() throws Exception {

        kafka.shutdown();
        zkServer.stop();
        Thread.sleep(1000);
    }
}
