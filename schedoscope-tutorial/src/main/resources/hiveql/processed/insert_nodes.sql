SELECT
  n.id,
  n.tstamp AS occurred_at,
  n.version,
  n.user_id,
  n.longitude,
  n.latitude,
  n.geohash,
  ${env}_schedoscope_example_osm_processed.collect(nt.key, nt.value) AS tags,
  '${workflow_time}' AS created_at,
  '${workflow_name}' AS created_by
FROM ${env}_schedoscope_example_osm_processed.nodes_with_geohash n
JOIN ${env}_schedoscope_example_osm_stage.node_tags nt
ON (n.id = nt.node_id)
WHERE year(n.tstamp) = '${year}'
  AND month(n.tstamp) = '${month}'
GROUP BY
  n.id,
  n.tstamp,
  n.version,
  n.user_id,  
  n.longitude,
  n.latitude,
  n.geohash,
  '${workflow_time}',
  '${workflow_name}'
