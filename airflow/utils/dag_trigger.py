import requests
from datetime import timedelta, datetime

# Define Airflow webserver details
airflow_url = "http://localhost:8080"  # Change to your Airflow webserver URL
airflow_username = "airflow"  # Use the username for Airflow
airflow_password = "airflow"  # Use the password for Airflow

# Define the DAG ID and the parameters to pass to the DAG
dag_id = "reddit_elt_pipeline"
dag_parameters = {
    "output_name": datetime.now().strftime("%Y%m%d"),
    "subreddit": "dataengineering",
    "time_filter": "year",
    "limit": 10000
}

# Construct the API endpoint for triggering the DAG
dag_trigger_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"

# Prepare the request data and headers
data = {
    "conf": dag_parameters
}

headers = {
    "Content-Type": "application/json"
}

# Send the POST request to trigger the DAG
response = requests.post(
    dag_trigger_url,
    json=data,
    auth=(airflow_username, airflow_password),
    headers=headers
)

if response.status_code == 200:
    print("DAG triggered successfully!", response.text)
else:
    print("Failed to trigger DAG:", response.status_code, response.text)
