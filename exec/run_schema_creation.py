import sys
import os

#making ingest module available
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(project_root)  # Add project root to sys.path

# importing ingest framework.
from ingestion.schema_utils.load_schema import SchemaLoader
from ingestion.schema_utils.create_schema import SchemaCreator, load_db_config

schema_definitions_path = os.getenv("schema_definitions_path")
db_config_path = os.getenv("db_config_path")

def main():
    load_yml = SchemaLoader("../ingestion/schemas/definitions/sinch_db/")
    tables_to_load = load_yml.load_tables()

    db_config = load_db_config('../docker/servers_local.json')
    create_tables = SchemaCreator(db_config)
    ddls = create_tables.generate_create_table_ddl(tables_to_load)

    for ddl in ddls:
        create_tables.create_table(ddl)
    create_tables.close()

if __name__ == "__main__":
    main()

