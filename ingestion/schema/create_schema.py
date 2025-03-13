import json
import logging
import os
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)
from ingestion.utils.db_connection import DatabaseConnection
from typing import Dict, Any


class SchemaCreator:
    def __init__(self, db_config: Dict[str, str]) -> None:
        """
        Initializes SchemaCreator with a single database connection.
        """
        self.db_connection = DatabaseConnection(**db_config)
        self.cursor = self.db_connection.connection.cursor()

    def generate_create_table_ddl(self, table_data: Dict[str, Any]) -> str:
        """Generates a CREATE TABLE statement."""
        schema_name = table_data["schema"]
        table_name = table_data["table"]
        columns = table_data["columns"]

        columns_sql = []
        for col_name, col_data in columns.items():
            col_type = col_data["type"]
            col_constraints = " ".join(col_data.get("constraints", []))
            columns_sql.append(f"{col_name} {col_type} {col_constraints}".strip())

        columns_sql_str = ",\n  ".join(columns_sql)
        ddl = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (\n  {columns_sql_str}\n);"
        return ddl

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


def load_db_config(config_path: str) -> Dict[str, Any]:
    """Loads database configuration from a relative or absolute path."""
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



#TESTING IN THE SAME SCRIPT
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
