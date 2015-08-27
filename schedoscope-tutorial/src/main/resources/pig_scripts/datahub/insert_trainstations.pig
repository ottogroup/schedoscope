in = LOAD '${input_table}' USING org.apache.hive.hcatalog.pig.HCatLoader() AS (id:chararray, occurred_at:chararray, version:int, user_id:int, longitude:double, latitude:double, geohash:chararray, tags:map[], created_at:chararray, created_by:chararray, year:chararray, month:chararray, month_id:chararray);
filter_stations = FILTER in BY (tags#'railway' == 'station') OR (tags#'railway' == 'halt');
select = FOREACH filter_stations GENERATE id, tags#'name' AS station_name:chararray, SUBSTRING(geohash,0,7) AS area, '${workflow_time}' AS created_at, '${workflow_name}' AS created_by;
out = STORE select INTO '${output_folders}' USING ${storage_format};
