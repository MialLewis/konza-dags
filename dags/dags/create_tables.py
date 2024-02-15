import trino
import sys
from trino.dbapi import connect

schema = sys.argv[1]
dim_ccd_raw = sys.argv[2]
user_query = sys.argv[3]
hostname = sys.argv[4]
account = sys.argv[5]
container = sys.argv[6]

conn = connect(
    host=hostname,
    port=8080,
    user="trino",
    catalog="hive",
    schema=schema,
)
cur = conn.cursor()
# TODO: change to use sqlalchemy
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema} WITH (location='abfs://{container}@{account}.dfs.core.windows.net/{schema}/')")
rows = cur.fetchall()
cur = conn.cursor()
cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{dim_ccd_raw} (ccd_json VARCHAR)") 
rows = cur.fetchall()
cur = conn.cursor()
cur.execute(f"CREATE TABLE IF NOT EXISTS {schema}.{user_query} (given VARCHAR, family VARCHAR, dob VARCHAR, gender VARCHAR)") 
rows = cur.fetchall()
cur = conn.cursor()
cur.execute(f"SELECT * FROM {schema}.{dim_ccd_raw} LIMIT 1") 
rows = cur.fetchall()
print(rows)
