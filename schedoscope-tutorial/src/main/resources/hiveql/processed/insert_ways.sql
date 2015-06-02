
SELECT
  tagged_way.id,
  tagged_way.occurredAt,
  tagged_way.version,
  tagged_way.user_id,
  tagged_way.tags,
  ${env}_schedoscope_example_osm_processed.collect(cast(wn.node_id AS STRING)) AS nodes,
  '${workflow_time}' AS createdAt,
  '${workflow_name}' AS createdBy
FROM (
  SELECT
    w.id,
    w.tstamp AS occurredAt,
    w.version,
    w.user_id,
    ${env}_schedoscope_example_osm_processed.collect(wt.key, wt.value) AS tags
  FROM ${env}_schedoscope_example_osm_stage.ways w
  JOIN ${env}_schedoscope_example_osm_stage.way_tags wt
    ON w.id = wt.way_id

  WHERE year(w.tstamp) = 2014
    AND month(w.tstamp) = 10
-- WHERE year(w.tstamp) = '${year}'
--   AND month(w.tstamp) = '${month}'
    
  GROUP BY
    w.id,
    w.tstamp,
    w.version,
    w.user_id
) tagged_way

JOIN ${env}_schedoscope_example_osm_stage.way_nodes wn
  ON tagged_way.id = wn.way_id

GROUP BY
  tagged_way.id,
  tagged_way.occurredAt,
  tagged_way.version,
  tagged_way.user_id,
  tagged_way.tags,
  '${workflow_time}',
  '${workflow_name}'
;

 
