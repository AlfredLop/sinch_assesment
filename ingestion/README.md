# LOAD_SCHEMA.py

## Overview
The `SchemaLoader` class is responsible for loading database schema definitions from YAML files. It reads, validates, and returns the schema definitions as Python dictionaries. This functionality is useful in automating schema creation and ensuring consistency in database structures.

## Class and Methods Explanation

### `SchemaLoader`
This class is responsible for reading schema definitions stored as YAML files in a specified directory and returning them as Python dictionaries.

#### **`__init__(self, schema_dir: str = "ingestion/schemas/definitions/sinch_db") -> None`**
**Purpose:**
- Initializes the `SchemaLoader` instance with the directory path containing YAML schema files.
- By default, it looks for schema definitions in `ingestion/schemas/definitions/sinch_db`.

**Parameters:**
- `schema_dir` (str): Path to the directory where schema YAML files are stored.

---

#### **`load_tables(self) -> List[Dict[str, Any]]`**
**Purpose:**
- Reads all YAML files in the specified directory.
- Validates if they contain essential metadata (like `database` and `schema`).
- Converts them into Python dictionaries.
- Returns a list of dictionaries representing table schemas.

**Steps:**
1. Initializes an empty list `tables` to store the parsed schema data.
2. Retrieves the absolute path of the schema directory and prints it.
3. Iterates over all files in the specified directory.
4. Checks if a file has a `.yml` extension.
5. Opens and parses the YAML file into a Python dictionary.
6. Validates that the required keys (`database` and `schema`) exist.
   - If not found, logs a warning and skips the file.
7. Appends the validated schema dictionary to the `tables` list.
8. Logs a success message if schemas are loaded correctly.
9. Returns the list of schema dictionaries.

**Returns:**
- `List[Dict[str, Any]]`: A list of dictionaries where each dictionary represents a table schema with column definitions, constraints, and metadata.

**Exception Handling:**
- `FileNotFoundError`: Logs an error and raises an exception if the schema directory is missing.
- `yaml.YAMLError`: Logs an error and raises an exception if there is an issue in parsing the YAML file.

---

### Standalone Execution
The script can be executed independently to test its functionality.

**`if __name__ == "__main__":`**
- Creates an instance of `SchemaLoader`, pointing to `schemas/definitions/sinch_db/`.
- Calls `load_tables()` to load and validate the schema files.
- This ensures that the script works correctly when run independently.

## Example YAML Schema File Structure
Below is an example of a YAML file that `SchemaLoader` expects to process:

```yaml
version: 2
database: postgres
schema: public
tables:
  - name: orders
    columns:
      order_id:
        type: VARCHAR(255)
        constraints: ["NOT NULL"]
      order_date:
        type: DATE
        constraints: ["NOT NULL"]
```

## Usage Example
```python
from schema_loader import SchemaLoader

schema_loader = SchemaLoader("schemas/definitions/sinch_db")
tables = schema_loader.load_tables()
print(tables)  # Prints the list of schema definitions as Python dictionaries
```

## Logging and Debugging
- The script logs detailed messages to track the loading process.
- If a schema file is missing critical metadata, it logs a warning and skips the file.
- Errors in parsing YAML files or missing directories are logged and raised as exceptions.

# VALIDATE_SCHEMA.py

## Overview
This module validates the PostgreSQL database schema against YAML-based schema definitions. It ensures that the actual database schema matches the expected structure, including data types and indexes. The `SchemaValidator` class is responsible for comparing column types, detecting missing or extra columns, and checking for expected indexes.

## Classes and Functions

### SchemaValidator
The `SchemaValidator` class is responsible for:
- Connecting to the PostgreSQL database.
- Retrieving schema definitions from the database.
- Comparing the actual database schema with YAML schema definitions.
- Identifying missing columns, type mismatches, and missing indexes.
- Logging the discrepancies found during validation.

#### `__init__(self, db_config: Dict[str, str], schema_loader: SchemaLoader)`
Initializes the `SchemaValidator` class by establishing a database connection and loading the schema definitions.

#### `normalize_type(self, db_type: str) -> str`
Normalizes PostgreSQL data types to a standard format for comparison. This is useful to ensure that equivalent types (e.g., `character varying` and `VARCHAR`) do not create false mismatches.

#### `get_database_schema(self, schema_name: str) -> Dict[str, Dict[str, str]]`
Retrieves the actual schema from the PostgreSQL database. Returns a dictionary where:
- The key is the table name.
- The value is another dictionary mapping column names to their data types.

This function queries the `information_schema.columns` table to get column names and data types.

#### `get_database_indexes(self, schema_name: str) -> Dict[str, List[str]]`
Retrieves the actual indexes from the PostgreSQL database. Returns a dictionary where:
- The key is the table name.
- The value is another dictionary mapping index names to their indexed columns.

It queries PostgreSQL system tables (`pg_index`, `pg_class`, `pg_attribute`, etc.) to fetch index information.

#### `validate_schema(self)`
Compares the YAML schema definitions with the actual database schema. It checks:
- If tables exist in the database.
- If expected columns exist in the database.
- If there are extra columns that are not defined in the YAML file.
- If the column data types match the expected types.
- If the indexes defined in YAML exist in the database.

For each table, it logs warnings for missing columns, extra columns, type mismatches, missing indexes, and extra indexes.

#### `close(self)`
Closes the database connection to ensure proper resource cleanup.

## Execution
The script can be executed as a standalone process to validate the schema. When run directly, it:
1. Loads the database configuration.
2. Initializes the `SchemaValidator` with the database connection and schema definitions.
3. Calls `validate_schema()` to compare the schema.
4. Closes the database connection.

```python
if __name__ == "__main__":
    db_config = load_db_config("../docker/servers_local.json")  
    print(db_config)

    schema_loader = SchemaLoader("schemas/definitions/sinch_db/")
    validator = SchemaValidator(db_config, schema_loader)

    try:
        validator.validate_schema()
    finally:
        validator.close()
```

## Expected Output
- **Warnings** for missing or extra columns.
- **Warnings** for type mismatches.
- **Warnings** for missing or extra indexes.
- **Info logs** for tables that match expected schema definitions.

This script helps maintain database integrity and ensures that the schema adheres to the expected structure before running data transformations or queries.

# CREATE_SCHEMA.py

## Overview
The `SchemaCreator` class is responsible for creating database tables dynamically based on YAML schema definitions. It reads configuration details from a JSON file, connects to a PostgreSQL database, and generates SQL statements to create tables, indexes, and partitions as specified in the YAML schema files.

## Class: `SchemaCreator`
This class handles the schema creation process, including defining columns, constraints, indexes, and partitions.

### **Methods**

### `__init__(self, db_config: Dict[str, Any])`
Initializes the `SchemaCreator` instance by establishing a connection to the PostgreSQL database.

- Uses the `DatabaseConnection` class to manage the connection.
- Initializes a cursor for executing queries.

---

### `generate_create_table_ddl(self, table_data: Dict[str, Any]) -> str`
Generates the SQL DDL (Data Definition Language) statements required to create tables.

#### Steps:
1. **Extracts Schema Information:**
   - Retrieves `schema_name`, `table_name`, and `columns` from the `table_data` dictionary.
   
2. **Defines Columns:**
   - Iterates through column definitions from the YAML file.
   - Constructs SQL column definitions, including data types and constraints (e.g., `NOT NULL`).
   
3. **Checks for Partitioning:**
   - Identifies partitioning type (e.g., RANGE, LIST, HASH) and the column to partition by.
   - If a partition exists, it constructs the partitioning clause.
   - Supports both `DATE` and `TIMESTAMP` formats.
   - Creates partitions for `2023` and `2024` dynamically.
   
4. **Generates Indexes:**
   - Checks for any defined indexes in the YAML schema and generates `CREATE INDEX` statements accordingly.
   
5. **Returns:**
   - A list of SQL statements including `CREATE TABLE` and index creation commands.

---

### `create_table(self, ddl_statement: str) -> None`
Executes the generated `CREATE TABLE` SQL statements in the PostgreSQL database.

#### Steps:
1. Logs the query being executed.
2. Runs the SQL statement using the database cursor.
3. Commits the changes to the database.
4. Handles errors and rolls back transactions in case of failure.

---

### `close(self) -> None`
Closes the database connection and cursor when schema creation is complete.

---

## Function: `load_db_config(config_path='docker/servers_local.json') -> Dict[str, Any]`
Reads the database connection parameters from a JSON configuration file.

#### Steps:
1. Loads the JSON file containing database credentials.
2. Extracts details such as `dbname`, `user`, `password`, `host`, and `port`.
3. Returns a dictionary with the connection details.

---

## **Standalone Execution**
If the script is run as a standalone program, it:
1. Loads the database configuration from `servers.json`.
2. Initializes a `SchemaCreator` instance.
3. Defines a sample table schema (e.g., `marketing` table).
4. Generates and executes the necessary SQL statements.
5. Closes the database connection.

---

## **Future Enhancements**
- **Automated Schema Evolution:**
  - Extend the script to detect schema changes and generate `ALTER TABLE` statements dynamically.
- **Support for Additional Partitioning Strategies:**
  - Implement automatic partition generation beyond just yearly partitions.
- **Parallel Execution:**
  - Optimize schema creation by executing multiple `CREATE TABLE` statements concurrently.
- **Error Logging & Monitoring:**
  - Integrate a logging framework to store detailed execution logs in a centralized system.

This `SchemaCreator` module is essential for dynamically managing database schemas in a structured and automated fashion, ensuring consistency between YAML definitions and the actual PostgreSQL database.


# LOAD_DATA.py

This document provides an in-depth explanation of the data ingestion pipeline implemented in Python. The pipeline reads CSV and TXT files, validates their structure based on YAML schema definitions, converts date formats where applicable, and ingests the processed data into a PostgreSQL database.

## Overview
The ingestion pipeline consists of the following key components:
- **FileLoader**: Reads and streams files from a directory.
- **DataValidator**: Validates the structure and contents of each file.
- **DataIngestor**: Inserts validated data into a PostgreSQL database.
- **convert_date_format**: Converts date values to a standardized format.

---

## Class: `FileLoader`
Handles reading files from a specified directory.

### Methods:
#### `__init__(self, directory: str)`
Initializes the `FileLoader` class with the directory path where data files are stored.

#### `get_files(self) -> List[str]`
Retrieves a list of valid CSV and TXT files from the directory and returns them as a list of file paths.

#### `stream_file(self, file_path: str) -> Generator[List[str], None, None]`
Streams a file line by line, yielding each row as a list of strings.

---

## Class: `DataValidator`
Validates file structure and content against the YAML schema.

### Methods:
#### `__init__(self, schema_loader: SchemaLoader)`
Initializes the `DataValidator` with an instance of `SchemaLoader` to load the schema definitions from YAML files.

#### `validate_structure(self, file_name: str, header: List[str]) -> Optional[Dict[str, Any]]`
Checks if the file columns match the expected YAML schema.
- Excludes `source_name` and `insert_date` from validation.
- Returns a dictionary of expected columns and their metadata if validation passes.

#### `validate_data(self, row: List[str], expected_columns: Dict[str, Any], source_name: str) -> bool`
Attempts to adjust date values in the row based on the expected format defined in the YAML schema.
- If a date conversion fails, logs a warning and keeps the original value.
- Returns `True` if validation succeeds; otherwise, returns `False`.

---

## Function: `convert_date_format(date_str: str, expected_format: str) -> Optional[str]`
Converts date values to a standard format (`YYYY-MM-DD`).
- Uses a predefined format mapping for common date formats (`DD/MM/YYYY`, `MM/DD/YYYY`, etc.).
- If conversion fails, logs a warning and returns the original string.

---

## Class: `DataIngestor`
Handles efficient data ingestion into PostgreSQL with batch processing.

### Methods:
#### `__init__(self, db_config: Dict[str, str], schema_loader: SchemaLoader)`
Initializes the `DataIngestor` with a PostgreSQL database connection and a `DataValidator` instance.

#### `ingest_file(self, file_path: str)`
Reads, validates, and inserts data from a given file into the database.
- Streams the file line by line.
- Validates column structure.
- Adds `source_name` and `insert_date` fields before insertion.
- Writes an error file **only** if invalid rows are found.

#### `insert_data(self, table_name: str, columns: List[str], rows: List[List[str]], file_path: str)`
Inserts processed data into the database in batches of `BATCH_SIZE`.
- Converts all values to strings before insertion.
- Appends `source_name` (filename) and `insert_date` (current timestamp) to each row.
- Uses `executemany` for batch insertion into PostgreSQL.

---

## Execution Flow
1. **Load Configuration:** Database credentials are loaded from a JSON file.
2. **Initialize Schema Loader:** The YAML schema definitions are read.
3. **Initialize Ingestor:** A `DataIngestor` instance is created with database and schema configurations.
4. **Retrieve Files:** The `FileLoader` retrieves a list of available files.
5. **Process Each File:**
   - The file is streamed line by line.
   - The structure is validated.
   - Data values are checked and transformed where necessary.
   - Valid rows are inserted into the database in batches.
   - Invalid rows are written to an error file (if necessary).

---

## Error Handling & Logging
- Errors during schema validation or data transformation are logged with detailed messages.
- Failed inserts due to invalid data are logged and stored in an error file.
- Each function includes error handling to prevent crashes during ingestion.

---

## Configuration Files
- **`servers_local.json`**: Stores database credentials.
- **YAML Schema Files**: Define the structure, constraints, and expected data types for each table.

---

## Future Enhancements
- Implement multithreading for parallel file processing.
- Add support for schema evolution (detecting and applying schema changes dynamically).
- Extend support for additional file formats (JSON, Parquet, etc.).
- Improve logging to include detailed statistics about ingestion performance.

---

## Running the Script
To execute the ingestion process, run:
```sh
python load_data.py
```
Ensure that the correct configurations are set in the `servers_local.json` file and that the source directory contains valid CSV/TXT files.

---

This document serves as a comprehensive guide to understanding and working with the ingestion pipeline. Let me know if you need any additional details or modifications!






