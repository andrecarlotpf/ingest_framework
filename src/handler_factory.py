from enum import Enum
from handlers import DBHandler, BigQueryHandler

class HandlerType(Enum):
    BIGQUERY = BigQueryHandler
    DATABASE = DBHandler


def get_handler(handler_type: str, **kwargs):
    try:
        handler_class = HandlerType[handler_type.upper()].value
        return handler_class(**kwargs)
    except KeyError:
        raise ValueError(f"Handler Type {handler_type} does not exists.")