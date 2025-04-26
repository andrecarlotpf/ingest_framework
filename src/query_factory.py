from enum import Enum

def incremental():
    return "incremental"
    

def full_load():
    return "full_load"


class LoadType(Enum):
    INCREMENTAL = incremental
    FULL_LOAD   = full_load


def format_columns(columns: list) -> str:
    return ", ".join(columns)


def query_factory(table_name: str, columns: list, load_type: LoadType, incremental_columns: str = None):
    query = load_type()
    




query_factory("tabela_muito_louca", ['id', 'name', 'timestamp_event'], LoadType.FULL_LOAD)