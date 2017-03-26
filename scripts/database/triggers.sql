CREATE OR REPLACE FUNCTION insert_new_dataset() RETURNS TRIGGER AS $$
  DECLARE
    last_updated_time timestamp;
    publisher_id integer;
  BEGIN
    IF (TG_OP = 'INSERT') THEN
      SELECT
        max(updated_time) INTO last_updated_time
      FROM dataset
      WHERE dataset.portal_dataset_id = NEW.portal_dataset_id
        AND dataset.portal_id = NEW.portal_id
      GROUP BY dataset.portal_dataset_id;

      IF (NOT FOUND) OR (last_updated_time < NEW.updated_time) THEN

        SELECT id INTO publisher_id FROM dataset_publisher WHERE name = NEW.publisher LIMIT 1;

        IF NOT FOUND THEN
          INSERT INTO dataset_publisher (name) VALUES (NEW.publisher)
          RETURNING id INTO publisher_id;
        END IF;

        INSERT INTO dataset (
          name, portal_dataset_id, created_time, updated_time, description,
          portal_link, data_link, publisher_id, portal_id, raw
        ) VALUES (
          NEW.name, NEW.portal_dataset_id, NEW.created_time, NEW.updated_time, NEW.description,
          NEW.portal_link, NEW.data_link, publisher_id, NEW.portal_id, NEW.raw
        ) RETURNING id INTO NEW.id;

        WITH existing_tags AS (
          SELECT id, name FROM dataset_tag WHERE name = any(NEW.tags)
        ), new_tags AS (
          INSERT INTO dataset_tag (name) (
            SELECT tag FROM unnest(NEW.tags) AS tag
            WHERE tag NOT IN (SELECT name FROM existing_tags)
            AND tag <> ''
          ) RETURNING id, name
        )
        INSERT INTO dataset_tag_xref (dataset_id, dataset_tag_id) (
          SELECT NEW.id, id FROM existing_tags
          UNION ALL
          SELECT NEW.id, id FROM new_tags
        );

        WITH existing_categories AS (
          SELECT id, name FROM dataset_category WHERE name = any(NEW.categories)
        ), new_categories AS (
          INSERT INTO dataset_category (name) (
            SELECT category FROM unnest(NEW.categories) AS category
            WHERE category NOT IN (SELECT name FROM existing_categories)
            AND category <> ''
          ) RETURNING id, name
        )
        INSERT INTO dataset_category_xref (dataset_id, dataset_category_id) (
          SELECT NEW.id, id FROM existing_categories
          UNION ALL
          SELECT NEW.id, id FROM new_categories
        );

      ELSE
        raise notice 'Dataset isn''t updated: %', NEW.portal_dataset_id;
      END IF;

      RETURN NEW;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_new_dataset
INSTEAD OF INSERT
ON view_latest_dataset
FOR EACH ROW
EXECUTE PROCEDURE insert_new_dataset();
