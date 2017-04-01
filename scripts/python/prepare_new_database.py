# This script is used to migrate data from the OpenDataDiscovery.org database
# to the Datarea.io database

import psycopg2 as pg
import os.path

basepath = os.path.dirname(__file__)

src_db = 'host=odd-main.cfoxcbrlgeta.us-east-1.rds.amazonaws.com port=5432 user=odd_admin password=Bko9tu39 dbname=odd'
dest_db = 'host=localhost port=5432 user=postgres password=9795388 dbname=datarea'

src_conn = pg.connect(src_db)
dest_conn = pg.connect(dest_db)

src_cur = src_conn.cursor()
dest_cur = dest_conn.cursor()

# parepare database
with open(os.path.join(basepath, '../database/tables.sql')) as sql_file:
    dest_cur.execute(sql_file.read())

with open(os.path.join(basepath, '../database/views.sql')) as sql_file:
    dest_cur.execute(sql_file.read())

with open(os.path.join(basepath, '../database/indexes.sql')) as sql_file:
    dest_cur.execute(sql_file.read())

with open(os.path.join(basepath, '../database/triggers.sql')) as sql_file:
    dest_cur.execute(sql_file.read())

dest_cur.execute('CREATE EXTENSION postgis')

# Add platform
src_cur.execute('SELECT name, website FROM platform')
results = src_cur.fetchall()

platforms = ["('%s', '%s')" % (platform[0], platform[1]) for platform in results]
sql = 'INSERT INTO platform (name, url) VALUES ' + ','.join(platforms)

dest_cur.execute(sql)

# Add portals
src_cur.execute('SELECT i.name, i.url, i.description, p.name FROM instance AS i LEFT JOIN platform AS p ON p.id = i.platform_id WHERE i.active')
results = src_cur.fetchall()

def escape(word):
    if word is None:
        return None

    return word.replace("'", "''")

portals = ["('%s','%s','%s',(SELECT id FROM platform WHERE name = '%s'))" % (escape(portal[0]), portal[1], escape(portal[2]), portal[3]) for portal in results]
sql = 'INSERT INTO portal (name, url, description, platform_id) VALUES ' + ','.join(portals)

dest_cur.execute(sql)

# Add Junar API keys
src_cur.execute('SELECT jii.api_url, jii.api_key, i.url FROM junar_instance_info AS jii LEFT JOIN instance AS i ON i.id = jii.instance_id WHERE i.active')
results = src_cur.fetchall();

portals = ["('%s', '%s', (SELECT id FROM portal WHERE url = '%s'))" % (result[0], result[1], result[2]) for result in results]
sql = 'INSERT INTO junar_portal_info (api_url, api_key, portal_id) VALUES ' + ','.join(portals)

dest_cur.execute(sql)

dest_conn.commit()

src_cur.close()
dest_cur.close()

src_conn.close()
dest_conn.close()
