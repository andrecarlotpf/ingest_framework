from utils.yaml_handler import YamlHandler
from utils.bq_handler import BigQueryHandler
from utils.database_handler import DBHandler


def main():
    x = YamlHandler("/root/PROJETOS/ingest_framework/src/teste.yaml")
    vars = x.parse_file_into_dict()

    query = f"""
        SELECT {", ".join(vars.get("columns"))} FROM `{vars.get("table_name")}`
    """

    bq = BigQueryHandler()

    result = bq.query_table(query)

    for row in result:
        print(row)



if __name__ == "__main__":
    main()
