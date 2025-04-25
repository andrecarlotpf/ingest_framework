from handler_factory import get_handler
from yaml_reader import YamlReader


def main():
    x = YamlReader("/root/PROJETOS/ingest_framework/src/teste.yaml")
    vars = x.parse_file_into_dict()

    query = f"""
        SELECT {", ".join(vars.get("columns"))} FROM `{vars.get("table_name")}`
    """

    bq = get_handler(
        "bigquery",
        **{
            "CREDENTIALS_PATH": "/root/PROJETOS/ingest_framework/bigquery-connector.json"
        },
    )

    result = bq.query_table(query)

    for row in result:
        print(row)


if __name__ == "__main__":
    main()
