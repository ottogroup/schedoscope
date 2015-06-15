SELECT 
  s.id,
  s.shop_name,
  s.shop_type,
  s.area,
  COALESCE(nearby_competitors,0) AS nearby_competitors,
  COALESCE(nearby_restaurants,0) AS nearby_restaurants,
  COALESCE(nearby_trainstations,0) AS nearby_trainstations,
  '${workflow_time}' AS created_at,
  '${workflow_name}' AS created_by
FROM ${env}_schedoscope_example_osm_datahub.shops s
LEFT OUTER JOIN (
  SELECT 
  COUNT(id)-1 AS nearby_competitors, -- don't count the shop itself
  area
  FROM ${env}_schedoscope_example_osm_datahub.shops
  GROUP BY area
) shops 
ON s.area = shops.area
LEFT OUTER JOIN (
  SELECT 
  COUNT(id) AS nearby_restaurants,
  area
  FROM ${env}_schedoscope_example_osm_datahub.restaurants
  GROUP BY area
) restaurants
ON s.area = restaurants.area
LEFT OUTER JOIN (
  SELECT 
  COUNT(id) AS nearby_trainstations,
  area
  FROM ${env}_schedoscope_example_osm_datahub.trainstations
  GROUP BY area
) trainstations
ON s.area = trainstations.area
