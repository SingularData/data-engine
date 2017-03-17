CREATE INDEX ON dataset (updated_time);
CREATE INDEX ON dataset (portal_dataset_id);
CREATE UNIQUE INDEX ON dataset (portal_id, portal_dataset_id, updated_time);
CREATE UNIQUE INDEX ON dataset_tag (name);
CREATE UNIQUE INDEX ON dataset_category (name);
CREATE UNIQUE INDEX ON dataset_publisher (name);
