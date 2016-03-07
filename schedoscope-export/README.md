## Schedoscope Export

Schedoscope Export is a collection of Map/Reduce jobs to move data from Hive (via HCatalog) into various output sinks. Currently the following sinks are supported:

 * JDBC 
 * Redis
 
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

#### Run the JDBC export

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
 
 * -d input database
 
 * -t input table
 
 * -i input filter, e.g. month='08' and year='2015'
 
 * -k key  column
 
 * -v value column, if empty full table export
 
 * -r optional key prefix
 
 * -c number of reducers, concurrency level
 
 * -a replace data for given key, only useful for native export of map/list types
 
 * -l pipeline mode for redis client

#### Run the Redis export

##### Run full table export
<pre>
yarn jar schedoscope-export-0.3.6-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d crichter_app_eci_datahub -t webtrends_event -h 'alananderson' -k id -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -c 10
</pre>

##### Run custom column export

<pre>
yarn jar schedoscope-export-0.3.6-SNAPSHOT-jar-with-dependencies.jar org.schedoscope.export.redis.RedisExportJob -d crichter_app_eci_datahub -t webtrends_event -h 'alananderson' -k id -v product_listing -s -p 'hive/_HOST@OTTOGROUP.COM' -m 'thrift://brentano.unbelievable-machine.net:9083'  -c 10
</pre>