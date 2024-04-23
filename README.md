# Data Engineering Subreddit Data Pipeline

This project fetches data from the "data engineering" subreddit using the Reddit API and processes it through a data pipeline orchestrated by Apache Airflow. The pipeline collects data every 24 hours, stores it in an Amazon S3 bucket, and then inserts it into Amazon Redshift for further analysis. A secondary task uses DBT to transform the data and create a new schema in Redshift, allowing for data visualization and KPIs on a dashboard.

## Project Components

- **Apache Airflow**: Orchestrates the data pipeline.
- **Reddit API**: Source of data.
- **Amazon S3**: Storage for raw data in Parquet format.
- **Amazon Redshift**: Data warehouse for structured data.
- **DBT (Data Build Tool)**: Used for data transformation and schema creation.
- **Docker**: For containerization and easy deployment.
- **Terraform**: Infrastructure as code for creating and destroying AWS resources.
- **PowerBI**: To visualize data collected from Reddit API

## Setup and Deployment

### Prerequisites

- Docker installed on your local machine.
- AWS account with necessary permissions to create S3 buckets and Redshift clusters.
- Terraform installed.

### Running the Pipeline

1. **Clone the Repository**

   Clone the repository to your local machine:

   ```
   git clone https://github.com/modhpranav/reddit-data-pipeline.git
   cd reddit-data-pipeline
   ```

2. **Terraform Setup**

   Navigate to the Terraform folder and initialize the resources:

   ```
   cd terraform
   terraform init
   terraform apply
   ```
   This will create the necessary AWS resources for S3 and Redshift.

3. **Docker Compose Setup**

   Build and run the Airflow environment using Docker Compose:

   ```
   docker compose up --build -d
   ```

### Ingesting Data from Reddit

File: ```dags/initiate_reddit_pipeline.py``` As the pipeline dag is triggered it will execute following tasks one by one.

File: ```utils/get_reddit_data.py``` Airflow is configured to run a DAG (Directed Acyclic Graph) that fetches data from Reddit every 24 hours / you can also trigger using ```utils/dag_trigger.py``` or from ```locahost:8080```.
File: ```utils/push_to_s3.py```  The data is stored in Parquet format in an S3 bucket.

### Inserting Data into Redshift

File: ```utils/push_to_redshift.py```  Once the data is in S3, Airflow triggers a task that inserts it into Amazon Redshift using `psycopg2`.

### Running DBT to Transform Data

Airflow triggers a DBT task to transform the data in Redshift, creating a new schema with specific filters (e.g., only today's data).

### Dashboard and KPIs Using PowerBI

After the data pipeline completes, the transformed data is used to create dashboard and display various KPIs.

## Cleaning Up Resources

To destroy the AWS resources created by Terraform:

  ```
  terraform destroy
  ```

This will remove all the resources created for this project.

## Notes and Considerations

- Ensure you have proper permissions to create and manage AWS resources.
- Be mindful of AWS costs, even with free-tier resources.
- Check your Reddit API access tokens and credentials to avoid rate limits or access issues.
- Keep your Docker environment and Terraform configuration secure.

## ToDO:
- I will soon create a pipeline which will use all the opensource/free resources possible to execute whole pipeline removing dependencies of redshift (alternate to BigQuery or in house postgreSQL) and other paid tools.

## Contributing and Feedback

Contributions are welcome! If you'd like to contribute, please submit a pull request. For issues or questions, open an issue in the GitHub repository.

## PowerBI Dashboard 
https://app.powerbi.com/groups/me/reports/e3e40765-b9d5-4d8c-b8dc-183fd08a3f31/ReportSection?experience=power-bi

## PowerBI Dashboard:
https://github.com/modhpranav/dbt-analytics/assets/47596415/54a40280-eb52-4bfe-9c1a-dbec80577c26

