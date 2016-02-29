CREATE TABLE `webtrends_struct`(
  `id` string, 
  `type` struct<field1:string, field2:string>)
COMMENT 'Datahub view of interpreted webtrends events'
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
LOAD DATA LOCAL INPATH '${DATA_FILE_PATH}' OVERWRITE INTO TABLE webtrends_struct PARTITION (year='2015', month='08', month_id='201508');
SELECT COUNT(*) FROM webtrends_struct;