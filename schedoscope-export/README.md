## Schedoscope Export

Schedoscope Export is a collection of Map/Reduce jobs to move data from Hive (via HCatalog) into various output sinks. Currently the following sinks are supported:

 * JDBC
 * Redis
 * Kafka
 * (S)FTP

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
yarn jar schedoscope-export-*-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.jdbc.JdbcExportJob -d default -t my_table -s -p 'hive/_HOST@PRINCIPAL.COM' -m 'thrift://metastore:9083' -c 10 -j 'jdbc:mysql://host/db' -k 1000 -u username -w mypassword
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
yarn jar schedoscope-export-*-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d default -t my_table -h 'redishost' -k id -s -p 'hive/_HOST@PRINCIPAL.COM' -m 'thrift://metastore:9083' -c 10
</pre>

##### Run custom column export
<pre>
yarn jar schedoscope-export-*-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d default -t my_table -h 'redishost' -k id -v products -s -p 'hive/_HOST@PRINCIPAL.COM' -m 'thrift://metastore:9083' -c 10
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
yarn jar schedoscope-export-*-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.kafka.KafkaExportJob -d default -t table -s -p 'hive/_HOST@PRINCIPAL.COM' -m 'thrift://metastore:9083' -k id -z zookeeper:2181 -b broker:9092
</pre>


### (S)FTP
This Map/Reduce job uploads files to a remote SFTP and FTP location. It supports user/pass authentication as well as user / key and user / key / passphrase authentication. The compression codecs one can use are gzip or bzip2 or none. The file format is either CSV or JSON.

#### Configuration options

 * -s set to true if kerberos is enabled

 * -m specify the metastore URIs

 * -p the kerberos principal

 * -d input database

 * -t input table

 * -i input filter, e.g. month='08' and year='2015'

 * -c number of reducers, concurrency level

 * -A a list of fields to anonymize separated by space, e.g. 'id visitor_id'

 * -S an optional salt to for anonymizing fields

 * -k the private ssh key file location

 * -u the (s)ftp user

 * -w the (s)ftp password or sftp passphrase

 * -j the (s)ftp endpoint, e.g. ftp://ftp.example.com:21/path

 * -q an optional file prefix to use, defaults to 'database-tablename-'

 * -l the delimiter to use for CSV files, defaults to '\t'

 * -h a flag to enable a header in CSV files

 * -x enable passive mode, only for ftp connections, defaults to 'true'

 * -z user dir is root, home dir on remote end is (s)ftp root dir

 * -g clean up hdfs dir after export, defaults to 'true'

 * -y the compression codec to use, one of 'gzip', 'bzip2' or 'none'

 * -v the file type to export, either 'csv' or 'json'

 #### Run the (S)FTP export
 <pre>
yarn jar schedoscope-export-*-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.ftp.FtpExportJob -d default -t table -s -p 'hive/_HOST@PRINCIPAL.COM' -m 'thrift://metastore:9083' -c 2 -u username -w mypassword -j 'ftp://ftp.example.com:21/path' -h -v json -y bzip2
 </pre>
