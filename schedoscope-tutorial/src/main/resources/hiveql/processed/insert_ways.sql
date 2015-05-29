SELECT 
  w.id,
  w.tstamp AS occurredAt,
  w.version,
  w.user_id,
  w.changeset_id
  ,
-- will be column of maps:
-- wt.key,
-- wt.value
  cast(wn.node_id AS STRING) AS node_id,
  wn.sequence_id,
  '${workflow_time}' AS createdAt,
  '${workflow_name}' AS createdBy
FROM ${env}_schedoscope_example_osm_stage.ways w
-- JOIN ${env}_schedoscope_example_osm_stage.way_tags wt
--   ON w.id = wt.node_id
JOIN ${env}_schedoscope_example_osm_stage.way_nodes wn
  ON w.id = wn.way_id
  
WHERE year(w.tstamp) = 2014
  AND month(w.tstamp) = 10
  
-- WHERE year(w.tstamp) = '${year}'
--   AND month(w.tstamp) = '${month}'
  
