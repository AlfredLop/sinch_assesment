import psycopg2
import logging
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DatabaseConnection:
    def __init__(self, dbname: str, user: str, password: str, host: str = "postgres", port: str = "5432") -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self._connection: Optional[psycopg2.extensions.connection] = None

    @property
    def connection(self) -> psycopg2.extensions.connection:
        """Lazy initialization of the database connection, connection not created until this attribute is called"""
        
        #making sure we reuse this connection if not closed.
        if self._connection is None or self._connection.closed:
            try:
                self._connection = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port
                )
                logging.info("Database connection established.")
            except psycopg2.Error as e:
                logging.error(f"Error connecting to the database: {e}")
                raise
        return self._connection

    def close(self) -> None:
        """Closes the database connection."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            logging.info("Database connection closed.")
