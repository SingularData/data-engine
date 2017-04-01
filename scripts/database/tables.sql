CREATE TABLE platform (
  id serial PRIMARY KEY,
  name text NOT NULL,
  url text NOT NULL,
  description text
);

CREATE TABLE portal (
  id serial PRIMARY KEY,
  name text NOT NULL,
  url text NOT NULL,
  platform_id integer REFERENCES platform (id),
  description text
);

CREATE TABLE junar_portal_info (
  id serial PRIMARY KEY,
  portal_id integer REFERENCES portal (id),
  api_url text NOT NULL,
  api_key text NOT NULL
);

CREATE TABLE region (
  id serial PRIMARY KEY,
  name text,
  geom geometry(MultiPolygon,4326)
);

CREATE TABLE dataset_publisher (
  id serial PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE dataset (
  id serial PRIMARY KEY,
  name text NOT NULL,
  portal_dataset_id text,
  created_time timestamp,
  updated_time timestamp NOT NULL,
  description text,
  portal_link text NOT NULL,
  data_link text,
  license text,
  publisher_id integer REFERENCES dataset_publisher (id),
  portal_id integer REFERENCES portal (id),
  region_id integer REFERENCES region (id),
  raw json NOT NULL
);

CREATE TABLE dataset_tag (
  id serial PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE dataset_tag_xref (
  id serial PRIMARY KEY,
  dataset_id integer REFERENCES dataset (id),
  dataset_tag_id integer REFERENCES dataset_tag (id)
);

CREATE TABLE dataset_category (
  id serial PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE dataset_category_xref (
  id serial PRIMARY KEY,
  dataset_id integer REFERENCES dataset (id),
  dataset_category_id integer REFERENCES dataset_category (id)
);
