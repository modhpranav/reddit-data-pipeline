import boto3
import botocore
import configparser
import pathlib

"""
Part of DAG. Take Reddit data and upload to S3 bucket. Takes one command line argument of format YYYYMMDD. 
This represents the file downloaded from Reddit, which will be in the /tmp folder.
"""

# Load AWS credentials
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
config_file = "config.conf"
parser.read(f"{script_path}/{config_file}")
print(f"Reading configuration file: {script_path}/{config_file}")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
AWS_REGION = parser.get("aws_config", "aws_region")





class PushToS3:
    def __init__(self, output_name: str):
        self.output_name = f"{output_name}.parquet"
    
    def connect_to_s3(self):
        """Connect to S3 Instance"""
        conn = boto3.resource("s3")
        self.conn = conn
    
    def create_bucket_if_not_exists(self):
        """Check if bucket exists and create if not"""
        exists = True
        try:
            self.conn.meta.client.head_bucket(Bucket=BUCKET_NAME)
        except botocore.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                exists = False
        if not exists:
            self.conn.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
    
    def upload_file_to_s3(self):
        """Upload file to S3 Bucket"""
        self.conn.meta.client.upload_file(
            Filename="/tmp/" + self.output_name, Bucket=BUCKET_NAME, Key=self.output_name
        )


    def run(self, status: dict):
        if status.get("status") == "failed":
            return status
        try:
            print("Connecting to S3")
            self.connect_to_s3()
            self.create_bucket_if_not_exists()
            self.upload_file_to_s3()
            status = {"status": "success", "message": "File uploaded to S3 successfully"}
        except Exception as e:
            status = {"status": "failed", "message": f"Error: {e}"}
        return status