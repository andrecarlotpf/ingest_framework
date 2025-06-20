from jinja2 import Environment, FileSystemLoader
from datetime import datetime
# TODO change paths to ENV variables

def generate_sql_origin_bq(**kwargs):
    kwargs = kwargs

    env = Environment(
        loader=FileSystemLoader(
            "/root/PROJETOS/ingest_framework/src/generator/templates"
        )
    )
    template = env.get_template("query_bq_source_template.sql.j2")

    # Render the SQL query
    query = template.render(
        columns=kwargs.get("columns"),
        table_name=kwargs.get("table_name"),
        filter_column=kwargs.get("incremental_field"),
        filter_value="123",
    )
    path = f"/root/PROJETOS/ingest_framework/src/generator/query/{kwargs.get('table_name').replace('.', '_')}_origin.sql"

    with open(path, "w+") as f:
        f.write(query)

    return path


def generate_sql_merge_snowflake(**kwargs):
    kwargs = kwargs

    env = Environment(
        loader=FileSystemLoader(
            "/root/PROJETOS/ingest_framework/src/generator/templates"
        )
    )
    template = env.get_template("query_template_merge_snflk.sql.j2")

    rendered_sql = template.render(
        target_table=kwargs.get("table_name"),
        primary_key=kwargs.get("primary_key"),
        columns=kwargs.get("columns"),
        # TODO change values to be dynamic
        values={"id": 123, "name": "Alice", "updated_at": "2025-04-30"},
    )

    path = f"/root/PROJETOS/ingest_framework/src/generator/query/{kwargs.get('table_name').replace('.', '_')}_merge_snowflake.sql"

    with open(path, "w+") as f:
        f.write(rendered_sql)

    return path


def generate_dag_ingest(**kwargs):
    env = Environment(loader=FileSystemLoader("/root/repos/ingest_framework/src/generator/templates"))
    template = env.get_template("dag_template.py.j2")

    output = template.render(
        owner="data_engineer",
        start_date=str(datetime.now()),
        dag_id="example_dag",
        schedule_interval="@daily",
        description="Example DAG generated from template",
        tags=[kwargs.get('source')]+kwargs.get('tags'),
        yaml_params=kwargs,
        task_id=f"ingest_{kwargs.get('table_name')}"
    )

    # Save to a .py file or print
    with open("./dags/generated_dag.py", "w") as f:
        f.write(output)