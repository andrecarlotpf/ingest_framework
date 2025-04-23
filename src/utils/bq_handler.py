from google.cloud import bigquery
from google.oauth2 import service_account
import os


class BigQueryHandler:
    def __init__(self):
        # key_path = os.environ["CREDENTIALS_PATH"]  "/root/PROJECTS/ingest_framework/bigquery-connector.json"
        key_path = "/root/PROJECTS/ingest_framework/bigquery-connector.json"
        credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

    def query_table(self, query: str):
        query_job = self.client.query(query)
        rows = query_job.result()

        return rows
