import psycopg2
import logging
import sys
import os
from typing import Dict, Any, List
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)
from ingestion.schema_utils.load_schema import SchemaLoader 
from ingestion.utils.db_connection import DatabaseConnection
from ingestion.schema_utils.create_schema import load_db_config


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s"
)


EQUIVALENT_TYPES = {
    "character varying": "VARCHAR",
    "text": "VARCHAR",
    "date": "DATE",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMPTZ"
}

class SchemaValidator:
    """
    A class to validate PostgreSQL schema against YAML definitions.

    Attributes:
        db (DatabaseConnection): PostgreSQL database connection.
        schema_loader (SchemaLoader): Instance to load expected schema from YAML files.
    """

    def __init__(self, db_config: Dict[str, str], schema_loader: SchemaLoader):
        """
        Initializes the schema validator.

        """
        self.db = DatabaseConnection(**db_config)
        self.schema_loader = schema_loader

    def normalize_type(self, db_type: str) -> str:
        """
        Converts PostgreSQL data types to standard SQL types for comparison.
        Strips length from VARCHAR(n) to avoid false mismatches.

        Args:
            db_type (str): The data type retrieved from PostgreSQL.

        Returns:
            str: Normalized data type.
        """
        db_type = db_type.lower()

        # Normalize VARCHAR(n) and remove length
        if db_type.startswith("character varying") or db_type.startswith("varchar"):
            return "VARCHAR"

        return EQUIVALENT_TYPES.get(db_type, db_type.upper())  # Normalize other types

    def get_database_schema(self, schema_name: str) -> Dict[str, Dict[str, str]]:
        """
        Retrieves the actual schema definition from PostgreSQL.

        Args:
            schema_name (str): The schema to validate.

        Returns:
            Dict[str, Dict[str, str]]: A dictionary where keys are table names and values are column definitions.
        """
        db_schema = {}
        query = """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s;
        """
        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute(query, (schema_name,))
                rows = cursor.fetchall()
                for table_name, column_name, data_type in rows:
                    if table_name not in db_schema:
                        db_schema[table_name] = {}
                    db_schema[table_name][column_name] = self.normalize_type(data_type)
        except psycopg2.Error as e:
            logging.error(f"Error retrieving schema from database: {e}")
            raise
        return db_schema

    def get_database_indexes(self, schema_name: str) -> Dict[str, List[str]]:
        """
        Retrieves only actual indexes from PostgreSQL (excluding tables and partitions).

        Args:
            schema_name (str): The schema to validate.

        Returns:
            Dict[str, List[str]]: A dictionary where keys are table names and values are lists of indexed columns.
        """
        indexes = {}
        query = """
            SELECT
                tab.relname AS table_name,
                idx.relname AS index_name,
                array_agg(att.attname) AS column_names
            FROM
                pg_index idx_info
                JOIN pg_class idx ON idx.oid = idx_info.indexrelid
                JOIN pg_class tab ON tab.oid = idx_info.indrelid
                JOIN pg_namespace ns ON ns.oid = tab.relnamespace
                JOIN pg_attribute att ON att.attrelid = tab.oid AND att.attnum = ANY(idx_info.indkey)
            WHERE
                ns.nspname = %s
            GROUP BY tab.relname, idx.relname;
        """
        try:
            with self.db.connection.cursor() as cursor:
                cursor.execute(query, (schema_name,))
                rows = cursor.fetchall()
                for table_name, index_name, column_names in rows:
                    if table_name not in indexes:
                        indexes[table_name] = {}
                    indexes[table_name][index_name] = column_names
        except psycopg2.Error as e:
            logging.error(f"Error retrieving indexes from database: {e}")
            raise
        return indexes

    def validate_schema(self):
        """
        Compares the YAML schema definitions with the actual database schema.
        Identifies missing, extra, or mismatched columns and indexes.
        """
        yaml_tables = self.schema_loader.load_tables()
        actual_schema = {}

        for yaml_table in yaml_tables:
            schema_name = yaml_table["schema"]
            table_name = yaml_table["table"]

            if schema_name not in actual_schema:
                actual_schema = self.get_database_schema(schema_name)

            if table_name not in actual_schema:
                logging.warning(f"Table '{table_name}' is missing in the database.")
                continue

            # Get expected and actual columns
            expected_columns = {col: self.normalize_type(details["type"]) for col, details in yaml_table["columns"].items()}
            actual_columns = actual_schema.get(table_name, {})

            # Column comparisons
            missing_columns = set(expected_columns) - set(actual_columns)
            extra_columns = set(actual_columns) - set(expected_columns)
            type_mismatches = {
                col: (expected_columns[col], actual_columns[col])
                for col in expected_columns if col in actual_columns and expected_columns[col] != actual_columns[col]
            }

            # Remove false positives where missing columns are actually present
            if not missing_columns:
                logging.info(f"All expected columns are present in '{table_name}'.")

            # Get actual indexes and compare
            actual_indexes = self.get_database_indexes(schema_name)
            db_indexes = actual_indexes.get(table_name, {})
            expected_indexes = {idx["name"]: idx["columns"] for idx in yaml_table.get("indexes", [])}

            missing_indexes = {name for name in expected_indexes if name not in db_indexes}
            extra_indexes = {name for name in db_indexes if name not in expected_indexes}

            # Print results
            logging.info(f"Validating table: {table_name}")
            if missing_columns:
                logging.warning(f"Missing columns in DB: {missing_columns}")
            if extra_columns:
                logging.warning(f"Extra columns in DB (not in YAML): {extra_columns}")
            if type_mismatches:
                logging.warning(f"Type mismatches: {type_mismatches}")
            if missing_indexes:
                logging.warning(f"Missing indexes: {missing_indexes}")
            if extra_indexes:
                logging.warning(f"Extra indexes in DB (not in YAML): {extra_indexes}")

    def close(self):
        """Closes the database connection."""
        self.db.close()



# Standalone execution
if __name__ == "__main__":
    db_config = load_db_config("../docker/servers_local.json")  
    print(db_config)

    schema_loader = SchemaLoader("schemas/definitions/sinch_db/")
    validator = SchemaValidator(db_config, schema_loader)

    try:
        validator.validate_schema()
    finally:
        validator.close()
