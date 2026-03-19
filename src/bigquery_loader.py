"""BigQuery data loader for GCP data warehouse pipelines."""

import os
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime


class BigQueryLoader:
    """Loads and manages data in Google BigQuery."""

    def __init__(self, project_id, dataset_name, credentials_path=None):
        """
        Initialize BigQuery client.

        Args:
            project_id: GCP project ID
            dataset_name: Target BigQuery dataset
            credentials_path: Path to service account JSON (optional)
        """
        if credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path
            )
            self.client = bigquery.Client(project=project_id, credentials=credentials)
        else:
            self.client = bigquery.Client(project=project_id)
        
        self.project_id = project_id
        self.dataset_name = dataset_name

    def create_dataset(self):
        """Create the dataset if it doesn't exist."""
        dataset_id = f"{self.project_id}.{self.dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        self.client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_id} created/confirmed.")

    def load_from_gcs(self, table_name, gcs_uri, source_format="PARQUET"):
        """
        Load data from Google Cloud Storage into BigQuery.

        Args:
            table_name: Target table name
            gcs_uri: GCS URI (e.g., gs://bucket/path/file.parquet)
            source_format: Source file format (PARQUET, CSV, JSON, AVRO)
        """
        table_id = f"{self.project_id}.{self.dataset_name}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            source_format=getattr(bigquery.SourceFormat, source_format),
            write_disposition="WRITE_APPEND",
        )

        if source_format == "PARQUET":
            job_config.autodetect = True

        load_job = self.client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        print(f"Loaded {load_job.output_rows} rows into {table_id}.")

    def run_query(self, query):
        """Execute a SQL query and return results."""
        query_job = self.client.query(query)
        return query_job.result()

    def insert_rows(self, table_name, rows):
        """
        Insert rows into a BigQuery table.

        Args:
            table_name: Target table name
            rows: List of dictionaries
        """
        table_id = f"{self.project_id}.{self.dataset_name}.{table_name}"
        errors = self.client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Encountered errors: {errors}")
        else:
            print(f"Inserted {len(rows)} rows into {table_id}.")
