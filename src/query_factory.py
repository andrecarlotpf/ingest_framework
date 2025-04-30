from jinja2 import Environment, FileSystemLoader
from google.cloud import storage
from google.oauth2 import service_account

def generate_sql_origin_bq(**kwargs):
    vars = kwargs

    env = Environment(loader=FileSystemLoader('/root/PROJETOS/ingest_framework/src/templates'))
    template = env.get_template('query_template.sql.j2')
 
    # Render the SQL query
    query = template.render(
        columns=vars.get("columns"),
        table_name=vars.get("table_name"),
        filter_column=vars.get("incremental_field"),
        filter_value="123"
    )
    path = f"/root/PROJETOS/ingest_framework/src/query/{vars.get('table_name').replace('.', '_')}_origin.sql"
    
    with open(path, "w+") as f:
        f.write(query)

    return path

def send_file_to_gcs(file_path, credentials):
    client = storage.Client(
            credentials=credentials, project=credentials.project_id
        )
    bucket = client.bucket("ingest_framework")

    blob = bucket.blob(f"config/query/origin/{file_path.split("/")[-1]}")
    blob.upload_from_filename(file_path)