SELECT 
id,
tags['name'] AS restaurant_name,
tags['cuisine'] AS restaurant_type,
substr(geohash,1,8) AS area,
-- for testing the area
tags['addr:postcode'] AS postcode,
'${workflow_time}' AS createdAt,
'${workflow_name}' AS createdBy

FROM ${env}_schedoscope_example_osm_processed.nodes
WHERE tags['amenity'] =	'restaurant'

