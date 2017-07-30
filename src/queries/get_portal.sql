SELECT
  p.id,
  p.name,
  p.url,
  p.description,
  pl.name AS platform,
  l.name AS location
FROM portal AS p
LEFT JOIN platform AS pl ON pl.id = p.platform_id
LEFT JOIN location AS l ON l.id = p.location_id
WHERE p.name = $1::text AND pl.name = $2::text
LIMIT 1
