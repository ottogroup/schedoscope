CREATE TABLE `test_arraystruct`(
  `id` string,
  `choice` array<struct<field1:string,field2:string,field3:string>>,
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
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
LOAD DATA LOCAL INPATH '${DATA_FILE_PATH}' OVERWRITE INTO TABLE test_arraystruct PARTITION (year='2015', month='08', month_id='201508');
SELECT COUNT(*) FROM test_arraystruct;