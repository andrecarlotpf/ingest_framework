from jinja2 import Environment, FileSystemLoader
from datetime import datetime
from pathlib import Path
from yaml_reader import YamlReader
import os


def __get_project_root() -> Path:
    return Path(__file__).parent


def __generate_sql_origin_bq(**kwargs):
    env = Environment(
        loader=FileSystemLoader(f"{__get_project_root()}/generator/templates")
    )
    template = env.get_template("query_bq_source_template.sql.j2")

    # Render the SQL query
    query = template.render(
        columns=kwargs.get("columns"),
        table_name=kwargs.get("table_name"),
        filter_column=kwargs.get("incremental_field"),
        # TODO get the most recent value in incremental field
        filter_value="123",
    )
    path = f"{__get_project_root()}/generator/query/{kwargs.get('table_name').replace('.', '_')}_source.sql"

    with open(path, "w+") as f:
        f.write(query)

    return path

def __generate_sql_merge_snowflake(**kwargs):
    kwargs = kwargs

    env = Environment(
        loader=FileSystemLoader(f"{__get_project_root()}/generator/templates")
    )
    template = env.get_template("query_template_merge_snflk.sql.j2")

    rendered_sql = template.render(
        target_table=kwargs.get("table_name"),
        primary_key=kwargs.get("primary_key"),
        columns=kwargs.get("columns"),
    )

    path = f"{__get_project_root()}/generator/query/{kwargs.get('table_name').replace('.', '_')}_merge.sql"

    with open(path, "w+") as f:
        f.write(rendered_sql)

    return path


def __generate_dag_ingest(**kwargs):
    env = Environment(
        loader=FileSystemLoader(f"{__get_project_root()}/generator/templates")
    )
    template = env.get_template("dag_template.py.j2")

    output = template.render(
        owner="data_engineer",
        start_date=str(datetime.now()),
        dag_id="example_dag",
        schedule_interval=None,
        description="Example DAG generated from template",
        tags=[kwargs.get("source")] + kwargs.get("tags"),
        yaml_params=kwargs,
        task_id=f"ingest_{kwargs.get('table_name')}",
        image_name="ingest_framework:latest",
        docker_url="unix://var/run/docker.sock",
    )

    path = f"{__get_project_root()}/generator/dags/dag_{kwargs.get('table_name').replace('.', '_')}.py"
    with open(path, "w") as f:
        f.write(output)


def generate_ingestion_files_bq(path):
    values = YamlReader(path).parse_file_into_dict()
    __generate_sql_origin_bq(**values)
    __generate_sql_merge_snowflake(**values)
    __generate_dag_ingest(**values)
    print("Finalizou")

import sys

generate_ingestion_files_bq(sys.argv[1])