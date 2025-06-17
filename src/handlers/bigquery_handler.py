from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

class BigQueryHandler:
    def __init__(self, **kwargs):
        self.table_name = kwargs.get('table_name')

    def query_table(self, credentials):
        query = f"SELECT * FROM `{self.table_name}`;"

        df = pd.read_gbq(query, credentials=credentials, project_id=credentials.project_id)
        return df

    def query_incremental(self):
        pass

    def query_full_load(self):
        pass
