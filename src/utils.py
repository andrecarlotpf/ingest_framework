from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent


from google.cloud import storage


def send_file_to_gcs(file_path, credentials, kwargs):
    client = storage.Client(credentials=credentials, project=credentials.project_id)
    # TODO change bucket name to get from ENV vars
    bucket = client.bucket("ingest_framework")
    # TODO change blob path to get from ENV vars
    blob = bucket.blob(
        f"config/query/{kwargs.get('table_name').replace('.', '_')}/{file_path.split('/')[-1]}"
    )

    blob.upload_from_filename(file_path)
