SELECT
  p.id,
  p.name,
  p.url,
  p.description,
  pl.name AS platform,
  l.name AS location,
  jpi.api_url,
  jpi.api_key
FROM portal AS p
LEFT JOIN platform AS pl ON pl.id = p.platform_id
LEFT JOIN location AS l ON l.id = p.location_id
LEFT JOIN junar_portal_info AS jpi ON jpi.portal_id = p.id
WHERE pl.name = $1::text
