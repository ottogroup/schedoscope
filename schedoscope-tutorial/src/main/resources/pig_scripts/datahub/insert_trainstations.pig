in = LOAD '${input_table}' USING org.apache.hcatalog.pig.HCatLoader() AS (id:chararray, occurred_at:chararray, version:int, user_id:int, longitude:double, latitude:double, geohash:chararray, tags:map[], created_at:chararray, created_by:chararray, year:chararray, month:chararray);
filter_stations = FILTER in BY (tags#'railway' == 'station') OR (tags#'railway' == 'halt');
select = FOREACH filter_stations GENERATE id, tags#'name' AS station_name:chararray, SUBSTRING(geohash,0,7) AS area;
out = STORE select INTO '${output_table}' using org.apache.hcatalog.pig.HCatStorer();
