import configparser
import pandas as pd
import pathlib
import praw
import logging
import numpy as np

"""
Part of Airflow DAG. Takes in one command line argument of format YYYYMMDD. 
Script will connect to Reddit API and extract top posts from past day
with no limit. For a small subreddit like Data Engineering, this should extract all posts
from the past 24 hours.
"""

# Read Configuration File
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "config.conf"
parser.read(f"{script_path}/{config_file}")
logging.info(f"Reading configuration file: {script_path}/{config_file}")
# Configuration Variables
SECRET = parser.get("reddit_config", "secret")
CLIENT_ID = parser.get("reddit_config", "client_id")

# Fields that will be extracted from Reddit.
# Check PRAW documentation for additional fields.
# NOTE: if you change these, you'll need to update the create table
# sql query in the upload_aws_redshift.py file
POST_FIELDS = (
    "id",
    "title",
    "score",
    "num_comments",
    "author",
    "created_utc",
    "url",
    "upvote_ratio",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
)


class GetRedditData:
    def __init__(self, output_name: str, subreddit: str, time_filter: str, limit):
        self.output_name = output_name
        self.subreddit = subreddit
        self.time_filter = time_filter
        self.limit = int(limit) if limit else None
        self.reddit_instance = self.api_connect()
    
    def api_connect(self):
        try:
            instance = praw.Reddit(
                client_id=CLIENT_ID, client_secret=SECRET, user_agent="My User Agent"
            )
            return instance
        except Exception as e:
            print(f"Unable to connect to API. Error: {e}")
            logging.error(f"Unable to connect to API. Error: {e}")
            return False
    
    def subreddit_posts(self):
        """Create posts object for Reddit instance"""
        subreddit = self.reddit_instance.subreddit(self.subreddit)
        self.posts = subreddit.top(time_filter=self.time_filter, limit=self.limit)

    def extract_data(self):
        """Extract Data to Pandas DataFrame object"""
        list_of_items = []
        for submission in self.posts:
            to_dict = vars(submission)
            sub_dict = {field: to_dict[field] for field in POST_FIELDS}
            list_of_items.append(sub_dict)
            self.extracted_data_df = pd.DataFrame(list_of_items)
    
    def transform_basic(self):
        """Some basic transformation of data. To be refactored at a later point."""

        # Convert epoch to UTC
        df = self.extracted_data_df
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s")
        # Fields don't appear to return as booleans (e.g. False or Epoch time). Needs further investigation but forcing as False or True for now.
        # TODO: Remove all but the edited line, as not necessary. For edited line, rather than force as boolean, keep date-time of last
        # edit and set all else to None.
        df["over_18"] = np.where(
            (df["over_18"] == "False") | (df["over_18"] == False), False, True
        ).astype(bool)
        df["edited"] = np.where(
            (df["edited"] == "False") | (df["edited"] == False), False, True
        ).astype(bool)
        df["spoiler"] = np.where(
            (df["spoiler"] == "False") | (df["spoiler"] == False), False, True
        ).astype(bool)
        df["stickied"] = np.where(
            (df["stickied"] == "False") | (df["stickied"] == False), False, True
        ).astype(bool)
        self.df = df

    
    def load_to_csv(self):
        """Save extracted data to CSV file in /tmp folder"""
        extracted_data_df = self.df
        extracted_data_df['id'] = extracted_data_df['id'].astype(str)
        extracted_data_df['title'] = extracted_data_df['title'].astype(str)
        extracted_data_df['author'] = extracted_data_df['author'].astype(str)
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
        ('id', pa.string()),  # Corresponds to VARCHAR in SQL
        ('title', pa.string()),  # Corresponds to VARCHAR(max) in SQL
        ('num_comments', pa.int32()),  # Corresponds to INT
        ('score', pa.int32()),  # Corresponds to INT
        ('author', pa.string()),  # Corresponds to VARCHAR(max)
        ('created_utc', pa.timestamp('us')),  # Corresponds to TIMESTAMP
        ('url', pa.string()),  # Corresponds to VARCHAR(max)
        ('upvote_ratio', pa.float32()),  # Corresponds to FLOAT
        ('over_18', pa.bool_()),  # Corresponds to BOOL
        ('edited', pa.bool_()),  # Corresponds to BOOL
        ('spoiler', pa.bool_()),  # Corresponds to BOOL
        ('stickied', pa.bool_()),  # Corresponds to BOOL
    ])
        table = pa.Table.from_pandas(extracted_data_df, schema=schema)
        pq.write_table(table, f"/tmp/{self.output_name}.parquet", coerce_timestamps='us', allow_truncated_timestamps=True)
        pq.write_table(table, f"/opt/airflow/outs/{self.output_name}.parquet", coerce_timestamps='us', allow_truncated_timestamps=True)

    def run(self):
        result = {"status": "Failed", "message": "Failed to extract data"}
        try:
            if self.reddit_instance:
                self.subreddit_posts()
                self.extract_data()
                self.transform_basic()
                self.load_to_csv()
                return {"status": "Success", "message": "Data extracted successfully"}
        except Exception as e:
            print(f"Error: {e}")
            logging.error(f"Error: {e}")
            return result