CREATE TABLE `test_array`(
  `id` string, 
  `type` array<string>)
COMMENT 'a test table using array as complex type'
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `month_id` string)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t' 
COLLECTION ITEMS TERMINATED BY ',' 
MAP KEYS TERMINATED BY ':' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'schema.checksum'='EBA465FA05431991234808D74CD59FD4', 
  'transient_lastDdlTime'='1449940102');
LOAD DATA LOCAL INPATH '${DATA_FILE_PATH}' OVERWRITE INTO TABLE test_array PARTITION (year='2015', month='08', month_id='201508');
SELECT COUNT(*) FROM test_array;