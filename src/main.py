from yaml_reader import YamlReader
from handlers.handler_factory import get_handler
from google.cloud import storage
from google.oauth2 import service_account
import yaml
import os
import pandas as pd

# Pegar as configuracoes da GCP
key_path = ''

def get_gcp_credentials():
    return service_account.Credentials.from_service_account_file(key_path)
def get_config(bucket_name, path):
    
    credentials = get_gcp_credentials()

    client = storage.Client(
        credentials=credentials, project=credentials.project_id
    )

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(path)
    content = blob.download_as_bytes()


    py_object = yaml.safe_load(content)
    return py_object # dict

# Criar o handler do tipo de ingestao

configs = get_config('ingest_framework', 'config/mercantil_framework/config.yaml')

handler = get_handler(**configs)

credentials = get_gcp_credentials()
result = handler.query_table(credentials)
print(result)
