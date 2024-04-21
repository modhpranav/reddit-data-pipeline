import configparser
import pathlib
import psycopg2
from psycopg2 import sql

"""
Part of DAG. Upload S3 CSV data to Redshift. Takes one argument of format YYYYMMDD. This is the name of 
the file to copy from S3. Script will load data into temporary table in Redshift, delete 
records with the same post ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Redshift will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/config.conf")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_cluster_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "reddit"


class PushToRedshift:

    def __init__(self, output_name: str):
        self.output_name = f"{output_name}.parquet"
        self.file_path = f"s3://{BUCKET_NAME}/{output_name}.parquet"
        self.role_string =  f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"
    
    def connect_to_redshift(self):
        """Connect to Redshift instance"""
        self.rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
    
    def create_table_if_not_exists(self):
        """Create table in Redshift if it doesn't exist"""
        sql_create_table = sql.SQL(
            """CREATE TABLE IF NOT EXISTS {table} (
                        id varchar PRIMARY KEY,
                        title varchar(max),
                        num_comments int,
                        score int,
                        author varchar(max),
                        created_utc timestamp,
                        url varchar(max),
                        upvote_ratio float,
                        over_18 bool,
                        edited bool,
                        spoiler bool,
                        stickied bool
                    );"""
                    ).format(table=sql.Identifier(TABLE_NAME))
        self.cur.execute(sql_create_table)
        
    def create_temp_table(self):
        """Create temporary table in Redshift"""
        create_temp_table = sql.SQL(
            "CREATE TEMP TABLE our_staging_table (LIKE {table});"
            ).format(table=sql.Identifier(TABLE_NAME))
        self.cur.execute(create_temp_table)
    
    def copy_data_to_temp_table(self):
        """Copy data from S3 to temporary table in Redshift"""
        sql_copy_to_temp = f"COPY our_staging_table FROM '{self.file_path}' iam_role '{self.role_string}' FORMAT AS PARQUET;"
        self.cur.execute(sql_copy_to_temp)
    
    def delete_dups(self):
        """Delete duplicate records from main table"""
        delete_from_table = sql.SQL(
            "DELETE FROM {table} USING our_staging_table WHERE {table}.id = our_staging_table.id;"
            ).format(table=sql.Identifier(TABLE_NAME))
        self.cur.execute(delete_from_table)

    def load_data_into_redshift(self):
        """Load data from temp table to main table"""
        insert_into_table = sql.SQL(
            "INSERT INTO {table} SELECT * FROM our_staging_table;"
            ).format(table=sql.Identifier(TABLE_NAME))
        self.cur.execute(insert_into_table)
    
    def drop_temp_table(self):
        """Drop temporary table"""
        drop_temp_table = "DROP TABLE our_staging_table;"
        self.cur.execute(drop_temp_table)
    
    def execute_queries(self):
        """Execute all queries"""
        with self.rs_conn:
            self.cur = self.rs_conn.cursor()
            self.create_table_if_not_exists()
            self.create_temp_table()
            self.copy_data_to_temp_table()
            self.delete_dups()
            self.load_data_into_redshift()
            self.drop_temp_table()
            self.rs_conn.commit()
    
    def run(self, status: dict):
        if status.get("status") == "failed":
            return status
        try:
            self.connect_to_redshift()
            self.execute_queries()
            return {"status": "Success", "message": "Data uploaded to Redshift successfully"}
        except Exception as e:
            print(f"Error: {e}")
            return {"status": "Failed", "message": "Failed to upload data to Redshift"}