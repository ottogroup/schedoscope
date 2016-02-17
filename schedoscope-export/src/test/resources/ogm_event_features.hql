CREATE TABLE `ogm_event_features`(
  `visitor_id` string, 
  `uri_path_hashed_count` map<string,bigint>, 
  `created_at` string, 
  `created_by` string)
PARTITIONED BY ( 
  `year` string, 
  `month` string, 
  `month_id` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
TBLPROPERTIES (
  'schema.checksum'='268A5DEE8D2AC2A757FBB8C5F1265B5C', 
  'transient_lastDdlTime'='1454943496');
LOAD DATA LOCAL INPATH '${DATA_FILE_PATH}' OVERWRITE INTO TABLE ogm_event_features PARTITION (year='2015', month='08', month_id='201508');
SELECT COUNT(*) FROM ogm_event_features;