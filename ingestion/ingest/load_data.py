import os
import csv
import logging
import psycopg2
import sys
from typing import Dict, List, Any, Optional, Generator
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.append(project_root)

from ingestion.schema_utils.load_schema import SchemaLoader
from ingestion.utils.db_connection import DatabaseConnection
from ingestion.schema_utils.create_schema import load_db_config

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s",
    force=True
)

SUPPORTED_FILE_TYPES = ["csv", "txt"]

BATCH_SIZE = 10000

def convert_date_format(date_str: str, expected_format: str) -> Optional[str]:
    """
    Converts a date from the given expected_format to 'YYYY-MM-DD'.
    If conversion fails, logs a warning and returns the original string.
    """
    format_mapping = {
        "DD/MM/YYYY": "%d/%m/%Y",
        "MM/DD/YYYY": "%m/%d/%Y",
        "YYYY-MM-DD": "%Y-%m-%d"
    }

    if expected_format not in format_mapping:
        logging.error(f"Unsupported date format in YAML: {expected_format}")
        return date_str

    try:
        return datetime.strptime(date_str, format_mapping[expected_format]).strftime("%Y-%m-%d")
    except ValueError:
        logging.warning(f"Invalid date format: {date_str}. Expected {expected_format}. Keeping original value.")
        return date_str

class FileLoader:
    """
    Handles reading files from a directory.
    """
    def __init__(self, directory: str):
        self.directory = directory

    def get_files(self) -> List[str]:
        """
        Retrieves a list of valid data files (CSV, TXT) from the directory.
        """
        files = [os.path.join(self.directory, f)
                 for f in os.listdir(self.directory)
                 if f.split('.')[-1] in SUPPORTED_FILE_TYPES]
        logging.info(f"Found {len(files)} files in {self.directory}")
        return files

    def stream_file(self, file_path: str) -> Generator[List[str], None, None]:
        """
        Streams a file line by line, yielding each row as a list.
        """
        try:
            with open(file_path, mode="r", encoding="utf-8") as file:
                reader = csv.reader(file)
                for row in reader:
                    yield row
        except Exception as e:
            logging.error(f"Error loading {file_path}: {e}", exc_info=True)

class DataValidator:
    """
    Validates file structure and attempts to adjust date values.
    It does not enforce strict data type validation for non-date fields.
    """
    def __init__(self, schema_loader: SchemaLoader):
        self.schema_loader = schema_loader

    def validate_structure(self, file_name: str, header: List[str]) -> Optional[Dict[str, Any]]:
        """
        Checks if file columns match the expected YAML schema.
        Excludes 'source_name' and 'insert_date' from the expected columns.
        """
        yaml_tables = self.schema_loader.load_tables()
        table_name = f'{os.path.splitext(os.path.basename(file_name))[0]}_raw'
        for table in yaml_tables:
            if table["table"] == table_name:
                expected = {col: table["columns"][col] 
                            for col in table["columns"] 
                            if col not in ('source_name', 'insert_date')}
                expected_keys = set(expected.keys())
                if set(expected_keys) != set(head.lower() for head in header):
                    logging.error(f"Schema mismatch in {file_name}. Expected: {list(expected_keys)}, Found: {header}")
                    return None
                return expected
        logging.error(f"No matching schema found for file: {file_name}")
        return None

    def validate_data(self, row: List[str], expected_columns: Dict[str, Any], source_name: str) -> bool:
        """
        Attempts to adjust date values in the row based on the YAML schema.
        If date conversion fails for a column, logs a warning and returns False so the row is marked invalid.
        Other fields are left as strings.
        """
        for i, (col, col_details) in enumerate(expected_columns.items()):
            if col_details.get("format") and col_details["type"].upper() == "DATE":
                converted = convert_date_format(row[i], col_details["format"])
                if converted is None:
                    logging.warning(f"Date conversion failed for column '{col}' with value '{row[i]}' in file {source_name}")
                    return False
                row[i] = converted
        return True

class DataIngestor:
    """
    Handles efficient data ingestion into PostgreSQL with batch processing.
    Adds source_name and insert_date fields before insertion.
    """
    def __init__(self, db_config: Dict[str, str], schema_loader: SchemaLoader):
        logging.info("Initializing DataIngestor")
        self.db = DatabaseConnection(**db_config)
        self.validator = DataValidator(schema_loader)

    def ingest_file(self, file_path: str) -> None:
        """
        Reads, validates, and inserts data from a file into the database.
        Adds source_name (filename) and insert_date (current UTC timestamp) to each row.
        Writes an error file only if any row fails date conversion.
        """
        logging.info(f"Starting ingestion for {file_path}")

        loader = FileLoader(os.path.dirname(file_path))
        stream = loader.stream_file(file_path)

        try:
            header = next(stream)
            logging.debug(f"File {file_path} header: {header}")
        except StopIteration:
            logging.error(f"Empty file: {file_path}")
            return

        table_name = f'{os.path.splitext(os.path.basename(file_path))[0]}_raw'
        expected_columns = self.validator.validate_structure(file_path, header)

        if not expected_columns:
            logging.error(f"Skipping file {file_path} due to schema mismatch.")
            return

        # Extend header with additional columns; these are not in the original file.
        header.extend(["source_name", "insert_date"])

        valid_batch = []
        error_rows = []  # Collect error rows in memory
        for row in stream:
            # Create a copy of the row to avoid modifying the original row in multiple iterations
            current_row = row.copy()
            if self.validator.validate_data(current_row, expected_columns, file_path):
                valid_batch.append(current_row)
            else:
                error_rows.append(row)
            if len(valid_batch) >= BATCH_SIZE:
                self.insert_data(table_name, header, valid_batch, file_path)
                valid_batch.clear()

        if valid_batch:
            self.insert_data(table_name, header, valid_batch, file_path)

        # Only write error file if there are errors
        if error_rows:
            invalid_file = f"{file_path}_errors.csv"
            with open(invalid_file, "w", encoding="utf-8", newline="") as err_file:
                error_writer = csv.writer(err_file)
                error_writer.writerow(header[:-2])  # Write original header (without extra fields)
                error_writer.writerows(error_rows)
            logging.warning(f"Saved {len(error_rows)} invalid rows to {invalid_file}")

    def insert_data(self, table_name: str, columns: List[str], rows: List[List[str]], file_path: str) -> None:
        """
        Inserts processed data into the database in batches.
        Ensures all values are converted to strings, and appends source_name and insert_date.
        """
        source_name = os.path.basename(file_path)
        current_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        # Convert all row values to strings and append source_name and insert_date
        processed_rows = [tuple(str(value) for value in row) + (source_name, current_timestamp) for row in rows]

        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        try:
            with self.db.connection.cursor() as cursor:
                cursor.executemany(query, processed_rows)
                self.db.connection.commit()
            logging.info(f"Inserted {len(rows)} rows into {table_name} (Source: {source_name})")
        except psycopg2.Error as e:
            logging.error(f"Database insert error in table {table_name}: {e}", exc_info=True)

if __name__ == "__main__":
    db_config = load_db_config("../docker/servers_local.json")
    schema_loader = SchemaLoader("schemas/definitions/sinch_db/")
    ingestor = DataIngestor(db_config, schema_loader)

    files = FileLoader("../data/to_process/").get_files()
    for file in files:
        ingestor.ingest_file(file)
