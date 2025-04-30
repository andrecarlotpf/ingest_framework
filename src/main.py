from yaml_reader import YamlReader
from query_factory import generate_sql_origin_bq, send_file_to_gcs


def main(yaml_path: str):
    x = YamlReader(yaml_path)
    vars = x.parse_file_into_dict()

    path = generate_sql_origin_bq(**vars)
    send_file_to_gcs(path)
    
if __name__ == "__main__":
    path = "/root/PROJETOS/ingest_framework/src/teste.yaml"
    main(path)
