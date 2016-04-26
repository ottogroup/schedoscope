## Schedoscope Export

Schedoscope Export is a collection of Map/Reduce jobs to move data from Hive (via HCatalog) into various output sinks. Currently the following sinks are supported:

 * JDBC
 * Redis
 * Kafka

### JDBC

This Map/Reduce job moves data into a relational database via a JDBC connection.

#### Configuration options

 * -s set to true if kerberos is enabled

 * -m specify the metastore URI

 * -p the kerberos principal

 * -j the jdbc connection string, jdbc:mysql://remote-host:3306/schema

 * -u the database user

 * -w the database password

 * -d input database

 * -t input table

 * -i input filter, e.g. month='08' and year='2015'

 * -e storage engine, either 'InnoDB' or 'MyISAM', works only for MySQL

 * -x columns to use for the 'DISTRIBUTE BY' clause, only Exasol

 * -c number of reducers, concurrency level

 * -k batch size for JDBC inserts

 * -A a list of fields to anonymize separated by space, e.g. 'id visitor_id'

 * -S an optional salt to for anonymizing fields

#### Run the JDBC export

The schedoscope-export project doesn't bundle any JDBC driver. It's necessary to add a JDBC driver to the classpath, the export job will copy into HDFS / distributed cache and add the driver to the classpath:

<pre>
export YARN_USER_CLASSPATH=/path/to/jdbc/jar/file/mysql-connector-java-5.1.38.jar
</pre>

After the classpath has been defined the JDBC export job can now be started:

<pre>
yarn jar target/schedoscope-export-0.3.6-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.jdbc.JdbcExportJob -d crichter_app_eci_datahub -t webtrends_event_small   -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -c 10 -j 'jdbc:mysql://alananderson/crichter' -k 1000  -u dev_crichter -w qweR1234
</pre>

### Redis

This Map/Reduce job moves data into Redis, it supports to modes:
 * a full table export in which a record is stored as a map in Redis indexed by a key

 * a key/value export which only exports the column specified indexed by a key

#### Configuration options

 * -s set to true if kerberos is enabled

 * -m specify the metastore URIs

 * -p the kerberos principal

 * -h Redis host

 * -P Redis port

 * -K Redis key space (default is 0)

 * -d input database

 * -t input table

 * -i input filter, e.g. month='08' and year='2015'

 * -k key  column

 * -v value column, if empty full table export

 * -r optional key prefix

 * -c number of reducers, concurrency level

 * -a replace data for given key, only useful for native export of map/list types

 * -l pipeline mode for redis client

 * -f flush redis key space

 * -A a list of fields to anonymize separated by space, e.g. 'id visitor_id'

 * -S an optional salt to for anonymizing fields

#### Run the Redis export

##### Run full table export
<pre>
yarn jar schedoscope-export-0.3.6-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d crichter_app_eci_datahub -t webtrends_event -h 'alananderson' -k id -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -c 10
</pre>

##### Run custom column export

<pre>
yarn jar schedoscope-export-0.3.6-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d crichter_app_eci_datahub -t webtrends_event -h 'alananderson' -k id -v product_listing -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -c 10
</pre>

### Kafka

This Map/Reduce job moves data into Kafka, using Avro schema underneath. The Avro schema is auto generated from the HCatalog schema. It supports 2 output encodings, though internally everything is handled as Avro:
 * JSON (plain string containing the payload as Json)

 * Avro (binary blob, containing the payload, the schema fingerprint is embedded in the byte stream)

#### Configuration options

 * -s set to true if kerberos is enabled

 * -m specify the metastore URIs

 * -p the kerberos principal

 * -d input database

 * -t input table

 * -i input filter, e.g. month='08' and year='2015'

 * -k key  column

 * -b the Kafka broker list, e.g. localhost1:9092,localhost2:9092

 * -z the zookeeper connect string, e.g. localhost1:2181,localhost2:2181

 * -P the producer type, either sync or async

 * -w cleanup policy, eiher delete or compact

 * -n number of partitions for the topic

 * -r replication factor for the partitions of the given topic

 * -c number of reducers, concurrency level

 * -x compression codec, either gzip, snappy or none

 * -o output encoding, either string or avro

 * -A a list of fields to anonymize separated by space, e.g. 'id visitor_id'

 * -S an optional salt to for anonymizing fields

#### Run the Kafka export
<pre>
yarn jar target/schedoscope-export-0.4.0-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.kafka.KafkaExportJob -d crichter_app_eci_datahub -t visit_small   -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -k id  -z akiyamatakashi.unbelievable-machine.net:2181 -b dongguan.unbelievable-machine.net:9092
</pre>
