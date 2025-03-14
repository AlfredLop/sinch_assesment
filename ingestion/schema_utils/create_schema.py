import json
import logging
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)
from ingestion.utils.db_connection import DatabaseConnection
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s"
)

class SchemaCreator:
    def __init__(self, db_config: Dict[str, str]) -> None:
        """
        Initializes SchemaCreator with a single database connection.
        """
        self.db_connection = DatabaseConnection(**db_config)
        self.cursor = self.db_connection.connection.cursor()

    def generate_create_table_ddl(self, table_data: Dict[str, Any]) -> str:
            ddl_scripts = []

            for table in table_data:
                """Generates a CREATE TABLE statement with support for indexes and partitions."""
                schema_name = table["schema"]
                table_name = table["table"]
                columns = table["columns"]

                # Generate column definitions
                columns_sql = []
                for col_name, col_data in columns.items():
                    col_type = col_data["type"]
                    col_constraints = " ".join(col_data.get("constraints", []))
                    columns_sql.append(f"{col_name} {col_type} {col_constraints}".strip())

                columns_sql_str = ",\n  ".join(columns_sql)

                # Check for partitioning
                partition_clause = ""
                partition_sql = []
                if "partition" in table:
                    partition = table["partition"]
                    partition_type = partition.get("type", "").upper()  # RANGE, LIST, or HASH
                    partition_column = partition.get("column", "")
                    date_format = partition.get("date_partition_format", "YYYY-MM-DD")  # Default to YYYY-MM-DD

                    if partition_type and partition_column:
                        partition_clause = f" PARTITION BY {partition_type} ({partition_column})"

                        # Adjust partition dates based on the format
                        if "HH24:MI:SS" in date_format:
                            from_date_2023 = "'2023-01-01 00:00:00'"
                            to_date_2023 = "'2023-12-31 23:59:59'"
                            from_date_2024 = "'2024-01-01 00:00:00'"
                            to_date_2024 = "'2024-12-31 23:59:59'"
                        else:
                            from_date_2023 = "'2023-01-01'"
                            to_date_2023 = "'2023-12-31'"
                            from_date_2024 = "'2024-01-01'"
                            to_date_2024 = "'2024-12-31'"

                        # Generate partition SQL with the correct format
                        if partition_type == "RANGE":
                            partition_sql.append(
                                f"CREATE TABLE {schema_name}.{table_name}_2023 "
                                f"PARTITION OF {schema_name}.{table_name} "
                                f"FOR VALUES FROM ({from_date_2023}) TO ({to_date_2023});"
                            )
                            partition_sql.append(
                                f"CREATE TABLE {schema_name}.{table_name}_2024 "
                                f"PARTITION OF {schema_name}.{table_name} "
                                f"FOR VALUES FROM ({from_date_2024}) TO ({to_date_2024});"
                            )

                # Generate CREATE TABLE DDL
                ddl = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n  {columns_sql_str}\n){partition_clause};"
                ddl_scripts.append(ddl)

                # Append partition creation statements (if applicable)
                ddl_scripts.extend(partition_sql)

                # Check for indexes (if present in YAML)
                if "indexes" in table:
                    for index in table["indexes"]:
                        index_name = index["name"]
                        index_columns = ", ".join(index["columns"])
                        ddl_scripts.append(f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({index_columns});")

            return ddl_scripts


    def create_table(self, ddl_statement:str) -> None:
        """Executes the SQL statement to create the table."""
        try:
            logging.info(f"Executing query: {ddl_statement}")
            self.cursor.execute(ddl_statement)
            self.db_connection.connection.commit()
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            self.db_connection.connection.rollback()

    def close(self) -> None:
        """Closes the database connection."""
        self.cursor.close()
        self.db_connection.close()


def load_db_config(config_path='docker/servers_local.json') -> Dict[str, Any]:
    """Parses json server config file and maps to the config var names accepted by the db class"""
    full_path = os.path.abspath(config_path)  # Resolve ../ to an absolute path
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Configuration file not found: {full_path}")
    
    with open(full_path, "r") as file:
        data = json.load(file)

    
    server_config = data["Servers"]["1"]  # Extract first server config
    return {
        "dbname": server_config["MaintenanceDB"],
        "user": server_config["Username"],
        "password": server_config["Password"],
        "host": server_config["Host"],
        "port": str(server_config["Port"])
    }



#this is for testing this standalone script
if __name__ == "__main__":


    db_config = load_db_config("../docker/servers.json")  # Ensure the correct path
    print(db_config)

    schema_creator = SchemaCreator(db_config)
    

    test_table = {
        "schema": "public",
        "table": "marketing",
        "columns": {
            "campaignid": {"type": "VARCHAR(255)", "constraints": ["NOT NULL"]},
            "targetaudience": {"type": "VARCHAR(255)"},
            "campaignstartdate": {"type": "DATE", "constraints": ["NOT NULL"], "format": "YYYY-MM-DD"},
            "campaignenddate": {"type": "DATE", "constraints": ["NOT NULL"], "format": "YYYY-MM-DD"}
        }
    }

    ddl = schema_creator.generate_create_table_ddl(test_table)
    schema_creator.create_table(ddl)
    schema_creator.close()
