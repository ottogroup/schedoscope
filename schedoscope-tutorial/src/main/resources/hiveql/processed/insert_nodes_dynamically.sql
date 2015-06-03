-- import org.schedoscope.dsl.transformations.HiveTransformation.insertDynamicallyInto

SELECT
  n.id,
  n.tstamp AS occurredAt,
  n.version,
  n.user_id,
  n.longitude,
  n.latitude,
  n.geohash,
  ${env}_schedoscope_example_osm_processed.collect(nt.key, nt.value) AS tags,
  '${workflow_time}' AS createdAt,
  '${workflow_name}' AS createdBy,
  year(n.tstamp) AS year,
  lpad(month(n.tstamp),2,'0') AS month
FROM ${env}_schedoscope_example_osm_processed.nodes_with_geohash n
JOIN ${env}_schedoscope_example_osm_stage.node_tags nt
    ON n.id = nt.node_id

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
-- DISTRIBUTE BY year(n.tstamp), month(n.tstamp)
