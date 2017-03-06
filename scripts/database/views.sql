CREATE VIEW view_latest_dataset AS
  WITH latest AS (
    SELECT DISTINCT ON (id) id FROM dataset
    ORDER BY id, updated_time DESC
  ), latest_tag AS (
    SELECT
      dtx.dataset_id,
      array_agg(t.name) AS tags
    FROM dataset_tag_xref AS dtx
    INNER JOIN latest AS l ON l.id = dtx.dataset_id
    LEFT JOIN dataset_tag AS t ON t.id = dtx.dataset_tag_id
    GROUP BY dtx.dataset_id
  ), latest_category AS (
    SELECT
      dcx.dataset_id,
      array_agg(c.name) AS categories
    FROM dataset_category_xref AS dcx
    INNER JOIN latest AS l ON l.id = dcx.dataset_id
    LEFT JOIN dataset_category AS c ON c.id = dcx.dataset_category_id
    GROUP BY dcx.dataset_id
  )
  SELECT
    d.id,
    d.portal_id,
    d.portal_dataset_id,
    d.name,
    d.created_time,
    d.updated_time,
    d.description,
    d.portal_link,
    d.data_link,
    d.license,
    p.name AS publisher,
    lt.tags,
    lc.categories,
    d.raw,
    r.geom
  FROM dataset AS d
  INNER JOIN latest AS l ON l.id = d.id
  LEFT JOIN dataset_publisher AS p ON p.id = d.publisher_id
  LEFT JOIN latest_tag AS lt ON lt.dataset_id = d.id
  LEFT JOIN latest_category AS lc ON lc.dataset_id = d.id
  LEFT JOIN region AS r ON r.id = d.region_id
