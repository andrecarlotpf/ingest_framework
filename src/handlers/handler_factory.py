from enum import Enum
from handlers import DBHandler, BigQueryHandler


class HandlerType(Enum):
    BIGQUERY = BigQueryHandler
    DATABASE = DBHandler


def get_handler(**kwargs):
    try:
        handler_class = HandlerType[kwargs.get('source').upper()].value
        return handler_class(**kwargs)
    except KeyError:
        raise ValueError(f"Handler Type {kwargs.get('source')} does not exists.")
