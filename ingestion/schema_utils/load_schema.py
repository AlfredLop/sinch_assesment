import yaml
import os
import logging
from typing import Dict, Any, List

class SchemaLoader:
    def __init__(self, schema_dir: str = "ingestion/schemas/definitions/sinch_db") -> None:
        self.schema_dir = schema_dir

    def load_tables(self) -> List[Dict[str, Any]]:
        """Loads all table YAML files and a list of python dics with each table fields and data types."""
        tables = []
        full_path = os.path.abspath(self.schema_dir)
        print(f"Checking directory: {full_path}")
        try:
            for filename in os.listdir(self.schema_dir):
                if filename.endswith(".yml"):
                    with open(os.path.join(self.schema_dir, filename), "r") as file:
                        table_data = yaml.safe_load(file)
                        if "database" not in table_data or "schema" not in table_data:
                            logging.warning(f"Skipping {filename}: Missing database/schema metadata.")
                            continue  # Skip files without metadata
                        tables.append(table_data)
            logging.info("Successfully loaded table schemas.")
        except FileNotFoundError as e:
            logging.error(f"Schema directory not found: {e}")
            raise
        except yaml.YAMLError as e:
            logging.error(f"Error parsing YAML: {e}")
            raise
        return tables

#this is for testing this standalone script assuming is run in the ingestion folder
if __name__ == "__main__":
    test = SchemaLoader("schemas/definitions/sinch_db/")
    test.load_tables()
