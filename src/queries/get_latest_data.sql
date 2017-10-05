SELECT
  d.identifier,
  d.title,
  d.description,
  dp.name AS publisher,
  dk.keyword,
  dt.theme,
  dd.distribution,
  json_build_object(
    'name', p.name,
    'platform', pl.name,
    'location', l.name,
    'description', p.description
  ) AS portal
FROM public.mview_latest_dataset AS mld
LEFT JOIN dataset AS d ON d.identifier = mld.identifier
LEFT JOIN dataset_portal_xref AS dpox ON dpox.dataset_id = d.id
LEFT JOIN portal AS p ON p.id = dpox.portal_id
LEFT JOIN location AS l ON l.id = p.location_id
LEFT JOIN platform AS pl ON pl.id = p.platform_id
LEFT JOIN dataset_publisher_xref AS dpux ON dpux.dataset_id = d.id
LEFT JOIN dataset_publisher AS dp ON dp.id = dpux.dataset_publisher_id
LEFT JOIN (
  SELECT
    dtx.dataset_id,
    COALESCE(array_agg(dt.name), '{}') AS theme
  FROM dataset_theme_xref AS dtx
  LEFT JOIN dataset_theme AS dt ON dt.id = dtx.dataset_theme_id
  GROUP BY dtx.dataset_id
) AS dt ON dt.dataset_id = d.id
LEFT JOIN (
  SELECT
    dkx.dataset_id,
    COALESCE(array_agg(dk.name), '{}') AS keyword
  FROM dataset_keyword_xref AS dkx
  LEFT JOIN dataset_keyword AS dk ON dk.id = dkx.dataset_keyword_id
  GROUP BY dkx.dataset_id
) AS dk ON dk.dataset_id = d.id
LEFT JOIN (
  SELECT
    dd.dataset_id,
    COALESCE(array_agg(json_build_object(
      'title', dd.title,
      'description', dd.description
    )), '{}') AS distribution
  FROM dataset_distribution AS dd
  GROUP BY dd.dataset_id
) AS dd ON dd.dataset_id = d.id
