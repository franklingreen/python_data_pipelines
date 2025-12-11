import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from system.database.azure_synapse import DatabaseHandler


@pytest.fixture
def handler():
    return DatabaseHandler(
        server="testserver",
        database="testdb",
        driver="ODBC Driver 18",
        sql_copt_access_token=1234,
        echo=False
    )


# --------------------------------------------------------------------------- #
# Token Handling
# --------------------------------------------------------------------------- #
def test_resolve_token_bytes(handler):
    token = b"abc123"
    assert handler._resolve_token(token) == token


def test_resolve_token_invalid(handler):
    with pytest.raises(TypeError):
        handler._resolve_token("invalid")


# --------------------------------------------------------------------------- #
# Engine String Construction
# --------------------------------------------------------------------------- #
def test_connection_string(handler):
    cs = handler._build_connection_string()
    assert "Driver=" in cs
    assert "Database=testdb" in urllib.parse.unquote_plus(cs)


# --------------------------------------------------------------------------- #
# Query Execution (Single Attempt)
# --------------------------------------------------------------------------- #
@patch("pandas.read_sql")
@patch("sqlalchemy.engine.Engine.connect")
@patch("sqlalchemy.create_engine")
def test_run_query_once(mock_engine, mock_connect, mock_read_sql, handler):
    mock_engine.return_value.connect.return_value = MagicMock()
    mock_read_sql.return_value = pd.DataFrame({"col": [1, 2]})

    df = handler._run_query_once("SELECT 1", b"token")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    mock_read_sql.assert_called_once()


# --------------------------------------------------------------------------- #
# Retry Logic
# --------------------------------------------------------------------------- #
@patch("pandas.read_sql", side_effect=Exception("connection failed"))
def test_retry_logic(mock_read, handler):
    with pytest.raises(ConnectionError):
        handler.read("SELECT 1", b"token", max_retries=3, sleep=0.1)


# --------------------------------------------------------------------------- #
# get_tables()
# --------------------------------------------------------------------------- #
@patch("sqlalchemy.inspect")
@patch("sqlalchemy.create_engine")
def test_get_tables(mock_engine, mock_inspect, handler):
    mock_inspect.return_value.get_table_names.return_value = ["table1", "table2"]

    tables = handler.get_tables(b"token")

    assert tables == ["table1", "table2"]
    mock_inspect.return_value.get_table_names.assert_called_once()


# --------------------------------------------------------------------------- #
# dispose()
# --------------------------------------------------------------------------- #
def test_dispose(handler):
    handler.conn = MagicMock()
    handler.db = MagicMock()

    handler.dispose()

    handler.conn.close.assert_called_once()
    handler.db.dispose.assert_called_once()
