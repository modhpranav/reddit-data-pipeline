from airflow.decorators import dag, task
import pendulum
import os
import sys

# Get the current file's directory
dag_dir = os.path.dirname(os.path.realpath(__file__))

# Add the 'utils' directory to 'sys.path'
utils_path = os.path.join(dag_dir, "..", "utils")
sys.path.append(utils_path)

from get_reddit_data import GetRedditData
from push_to_s3 import PushToS3
from push_to_redshift import PushToRedshift


@dag(
    dag_id="reddit_elt_pipeline",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2024, 2, 1),
    catchup=False
)
def reddit_elt_pipeline(output_name: str, subreddit: str, time_filter: str, limit):

    @task()
    def fetch_data(output_name: str, subreddit: str, time_filter: str, limit):
        """Fetch data from Reddit API"""
        fetch_data.doc_md = "Fetch data from Reddit API"
        return GetRedditData(output_name, subreddit, time_filter, limit).run()

    @task()
    def push_to_s3(output_name: str, data_status: dict):
        """Push Reddit data to S3"""
        push_to_s3.doc_md = "Push Reddit data to S3"
        return PushToS3(output_name).run(data_status)
    
    @task()
    def update_redshift(output_name: str, s3_status: dict):
        """Update Redshift table with Reddit data"""
        update_redshift.doc_md = "Update Redshift table with Reddit data"
        return PushToRedshift(output_name).run(s3_status)

    data_status = fetch_data(output_name, subreddit, time_filter, limit)
    s3_status = push_to_s3(output_name, data_status)
    redshift_status = update_redshift(output_name, s3_status)


    return redshift_status



reddit_elt_pipeline_execution = reddit_elt_pipeline("20240422", "dataengineering", "day", None)