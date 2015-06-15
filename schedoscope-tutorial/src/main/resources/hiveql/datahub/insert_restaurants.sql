SELECT 
id,
tags['name'] AS restaurant_name,
tags['cuisine'] AS restaurant_type,
substr(geohash,1,7) AS area,
'${workflow_time}' AS created_at,
'${workflow_name}' AS created_by

FROM ${env}_schedoscope_example_osm_processed.nodes
WHERE tags['amenity'] =	'restaurant'

