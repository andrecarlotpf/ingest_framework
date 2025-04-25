from sqlalchemy import create_engine
from sqlalchemy import Engine
import pandas as pd


class DBHandler:
    def __init__(self, **kwargs):
        driver = kwargs.get("driver")
        host = kwargs.get("host")
        port = kwargs.get("port")
        database = kwargs.get("database")
        user = kwargs.get("user")
        password = kwargs.get("password")
        extras = kwargs.get("extras", None)

        self.uri_connection = (
            f"{driver}://{user}:{password}@{host}:{port}/{database}?{'?'.join(extras)}"
        )
        print(self.uri_connection)

    def __create_engine(self) -> Engine:
        engine = create_engine(self.uri_connection)
        return engine

    def execute_query(self, query: str) -> pd.DataFrame:
        engine = self.__create_engine()

        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            return df
