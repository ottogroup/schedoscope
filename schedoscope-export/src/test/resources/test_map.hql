CREATE TABLE `test_map`(
  `id` string, 
  `type` map<string,bigint>, 
  `created_at` string, 
  `created_by` string)
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
  'schema.checksum'='268A5DEE8D2AC2A757FBB8C5F1265B5C', 
  'transient_lastDdlTime'='1454943496');
LOAD DATA LOCAL INPATH '${DATA_FILE_PATH}' OVERWRITE INTO TABLE test_map PARTITION (year='2015', month='08', month_id='201508');
SELECT COUNT(*) FROM test_map;