import sys
import os

#making ingest module available
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)  # Add project root to sys.path

# importing ingest framework.
from ingestion.schema_utils.load_schema import SchemaLoader
from ingestion.schema_utils.create_schema import SchemaCreator, load_db_config
from ingestion.ingest.load_data import DataIngestor, FileLoader

schema_definitions_path = os.getenv("schema_definitions_path")
db_config_path = os.getenv("db_config_path")

def main():
    load_yml = SchemaLoader("../ingestion/schemas/definitions/sinch_db/")
    db_config = load_db_config('../docker/servers_local.json')
    
    ingestor = DataIngestor(db_config, load_yml)

    files = FileLoader("../data/to_process/").get_files()
    for file in files:
        ingestor.ingest_file(file)

if __name__ == "__main__":
    main()
