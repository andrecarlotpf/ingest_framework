from yaml_reader import YamlReader
from generator.query_generator import (
    generate_sql_origin_bq,
    generate_sql_merge_snowflake,
)
from google.cloud import storage

def send_file_to_gcs(file_path, credentials):
    client = storage.Client(credentials=credentials, project=credentials.project_id)
    # TODO change bucket name to get from ENV vars
    bucket = client.bucket("ingest_framework")
    # TODO change blob path to get from ENV vars
    blob = bucket.blob(f"config/query/origin/{file_path.split('/')[-1]}")
    
    blob.upload_from_filename(file_path)


def main(yaml_path: str):
    x = YamlReader(yaml_path)
    vars = x.parse_file_into_dict()
    generate_sql_merge_snowflake(**vars)
    
if __name__ == "__main__":
    path = "/root/PROJETOS/ingest_framework/src/teste.yaml"
    main(path)
