import time
import datetime
import urllib.parse
import pandas as pd
from timeit import default_timer as timer
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError, ProgrammingError

from system.database.service_principal_token import MicrosoftGraph
from system.printer import Now, DataFramePrint


class DataWarehouse:
    server: str
    database: str
    driver: str
    sql_copt_access_token: int


class DatabaseHandler:
    def __init__(
        self,
        server: str,
        database: str,
        driver: str,
        sql_copt_access_token: int,
        timeout: int = 30,
        echo: bool = False,
        printer=None,
        frame_printer=None
    ):
        self.server = server
        self.database = database
        self.driver = driver
        self.sql_copt_access_token = sql_copt_access_token
        self.timeout = timeout
        self.echo = echo

        self.printer = printer or Now(process="AzureSynapseHandler", script="")
        self.frame_printer = frame_printer or DataFramePrint()

        self.db = None
        self.conn = None

    @staticmethod
    def _resolve_token(token) -> bytes:
        if isinstance(token, MicrosoftGraph):
            return token.get_token()
        elif isinstance(token, bytes):
            return token
        raise TypeError(f"Unsupported token type: {type(token)}")

    def _build_connection_string(self) -> str:
        conn = (
            f"Driver={self.driver};"
            f"Server=tcp:{self.server},1433;"
            f"Database={self.database};"
            f"TrustServerCertificate=no;"
            f"Connection Timeout={self.timeout};"
            f"autocommit=True;"
        )
        return urllib.parse.quote_plus(conn)

    def _create_engine(self, token: bytes):
        conn_str = self._build_connection_string()
        return create_engine(
            f"mssql+pyodbc:///?odbc_connect={conn_str}",
            pool_pre_ping=True,
            connect_args={
                "attrs_before": {self.sql_copt_access_token: token},
                "autocommit": True,
            },
        )

    def _run_query_once(self, sql: str, token: bytes) -> pd.DataFrame:
        self.db = self._create_engine(token)
        self.conn = self.db.connect()
        try:
            return pd.read_sql(sql, self.conn)
        finally:
            self.dispose()

    def read(
        self,
        sql: str,
        token,
        *,
        max_retries: int = 5,
        sleep: int = 10,
        retry_on_zero: bool = False,
        print_frame: bool = False,
    ) -> pd.DataFrame:

        attempt = 0
        resolved = self._resolve_token(token)
        start = timer()

        while attempt < max_retries:
            try:
                df = self._run_query_once(sql, resolved)

                if df.empty and retry_on_zero:
                    attempt += 1
                    self.printer.print("0 rows returned", f"retrying in {sleep}s")
                    time.sleep(sleep)
                    continue

                if print_frame:
                    self.frame_printer.print(df)

                duration = str(datetime.timedelta(seconds=timer() - start))
                self.printer.print("Query time:", duration)

                return df

            except ProgrammingError as pe:
                raise
            except SQLAlchemyError:
                self.printer.print_warning(f"Connection attempt {attempt+1}/{max_retries} failed")
                attempt += 1
                time.sleep(sleep)

        raise ConnectionError(
            f"Failed to execute query on {self.server} after {max_retries} attempts."
        )

    def dispose(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        if self.db is not None:
            try:
                self.db.dispose()
            except Exception:
                pass
        self.conn = None
        self.db = None
