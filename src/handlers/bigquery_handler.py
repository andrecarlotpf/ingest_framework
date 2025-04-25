from google.cloud import bigquery
from google.oauth2 import service_account


class BigQueryHandler:
    def __init__(self, **kwargs):
        # TO DO CRIAR A VARIAVEL DE LOCALIZACAO DA CHAVE SENDO PASSADA NO INSTANCIAMENTO DA CLASSE
        key_path = kwargs.get("CREDENTIALS_PATH")
        credentials = service_account.Credentials.from_service_account_file(key_path)

        self.client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

    def query_table(self, query: str):
        query_job = self.client.query(query)
        rows = query_job.result()

        return rows
    
    def query_incremental(self):
        pass
    
    def query_full_load(self):
        pass
