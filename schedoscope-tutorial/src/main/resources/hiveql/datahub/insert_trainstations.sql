SELECT 
id,
tags['name'] AS station_name,
substr(geohash,1,7) AS area,
'${workflow_time}' AS createdAt,
'${workflow_name}' AS createdBy

FROM ${env}_schedoscope_example_osm_processed.nodes
WHERE tags['railway'] = 'station' 
  OR tags['railway'] = 'halt' 
