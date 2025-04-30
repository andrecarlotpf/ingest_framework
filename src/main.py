from yaml_reader import YamlReader
from query_factory import generate_sql_origin_bq


def main(yaml_path: str):
    x = YamlReader(yaml_path)
    vars = x.parse_file_into_dict()

    generate_sql_origin_bq(**vars)

    
if __name__ == "__main__":
    path = "/root/PROJETOS/ingest_framework/src/teste.yaml"
    main(path)
