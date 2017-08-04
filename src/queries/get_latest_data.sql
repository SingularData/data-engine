SELECT
  uuid,
  mld.name,
  mld.description,
  publisher,
  mld.url,
  tags,
  categories,
  created,
  updated,
  license,
  files,
  region_name,
  json_build_object(
    'name', portal,
    'platform', platform,
    'location', l.name,
    'description', p.description
  ) AS portal
FROM public.mview_latest_dataset AS mld
LEFT JOIN portal AS p ON p.name = mld.portal
LEFT JOIN location AS l ON p.location_id = l.id
