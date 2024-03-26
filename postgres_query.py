import os
import sys
import subprocess
from google.cloud.sql.connector import Connector
import sqlalchemy

# set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'genotools-02f64a1e10be.json'

# connection variables
project_id = 'genotools'
region = 'us-central1'
instance_name = 'genotools'

INSTANCE_CONNECTION_NAME = f'{project_id}:{region}:{instance_name}'

DB_USER = 'postgres'
DB_PASS = 'genotools'
DB_NAME = 'snp_metrics'

# initialize gcloud sql connector
connector = Connector()

# get connection
def getconn():
    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME
    )
    return conn

# create and sqlalchemy engine
pool = sqlalchemy.create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

# connect
db_conn = pool.connect()

# query and fetch snps table
triple_join_count = db_conn.execute("select count(*) from samples").fetchall()
print(triple_join_count)

# close connection
connector.close()