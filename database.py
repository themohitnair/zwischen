import duckdb
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger(__name__)

DATABASE_FILE = "zwischen.duckdb"

def init_zwischen_db() -> None:
    """
    Initializes the log table if it doesn't exist and the database file itself.

    args: None
    returns: None
    """
    with yield_conn() as db:
        create_log = """
        CREATE TABLE IF NOT EXISTS log (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            method VARCHAR,
            ip VARCHAR,
            city VARCHAR,
            country VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            endpoint VARCHAR,
            status_code INTEGER,
            browser VARCHAR,
            os VARCHAR, 
            device VARCHAR,
            referrer VARCHAR
        )
        """
        db.execute(create_log)
        db.commit()
        logger.info("DuckDB Tables Created.")

def yield_conn() -> duckdb.DuckDBPyConnection:
    """ 
    Yields a connection to the DuckDB Database.

    args: None
    returns: duckdb.DuckDBPyConnection
    """
    conn = duckdb.connect(database=DATABASE_FILE, read_only=False)
    return conn

def create_serial_sequence(db: duckdb.DuckDBPyConnection):
    try:
        create_seq = """ 
        CREATE SEQUENCE serial;
        """

        db.execute(create_seq)
    except duckdb.Error as e:
        logger.error(f"Database Error: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected Error: {e}")
        return {"error": str(e)}