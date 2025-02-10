# MDBX Data Compare

MDBX Data Compare represents a CLI tool that is able to:

- Compare two MDBX databases to ensure they have the same entries
- List available tables within a single MDBX database
- Log all entries for a given table in an MDBX database

It offers three CLI commands:
- `compare` – compares two databases
- `tables` – lists database tables
- `entries` – logs entries in a specific table

## CLI Commands Documentation

This paragraph documents the CLI commands that are incorporated into the MDBX Data Compare tool.

### Commands

#### `compare`

It is used to compare two MDBX databases by reading all entries from a specified table and ensuring they match (both in terms of number of records and content).

- **Name**: `compare`
- **Usage**: Compares two MDBX databases.
- **Action**: `runCompare`
- **Flags**:
    - `datadir1`:
        - **Name**: `datadir1`
        - **Usage**: Data directory of the first database.
        - **Destination**: `&dataDir1`
    - `datadir2`:
        - **Name**: `datadir2`
        - **Usage**: Data directory of the second database.
        - **Destination**: `&dataDir2`
    - `table`:
        - **Name**: `table`
        - **Usage**: Name of the database table to compare.
        - **Destination**: `&dbTable`

#### `tables`

It is used to list the names of all tables in a single MDBX database, along with the number of entries in each table.

- **Name**: `tables`
- **Usage**: Logs available MDBX database tables.
- **Action**: `runTables`
- **Flags**:
    - `datadir`:
        - **Name**: `datadir`
        - **Usage**: Data directory to scan for tables.
        - **Destination**: `&dataDir`

#### `entries`

It is used to log all entries (keys and values) of a specified table within a single MDBX database.

- **Name**: `entries`
- **Usage**: Logs entries for an MDBX database table.
- **Action**: `runEntries`
- **Flags**:
    - `datadir`:
        - **Name**: `datadir`
        - **Usage**: Data directory containing the MDBX database.
        - **Destination**: `&dataDir`
    - `table`:
        - **Name**: `table`
        - **Usage**: Name of the database table whose entries will be logged.
        - **Destination**: `&dbTable`

### Example Usage

**Pre-requisite:** Navigate to the directory containing this tool and run:

`compare` Command
```sh
compare --datadir1 </path/to/mdbx1> --datadir2 </path/to/mdbx2>--table <table_name>
```
Checks if SomeTable exists in both databases with identical keys and values.

`tables` Command:
```sh
tables --datadir </path/to/mdbx>
```

Lists all tables found in the MDBX database located in /path/to/mdbx, along with the number of entries in each table.

`entries` Command:
```sh
entries --datadir </path/to/mdbx> --table <table_name>
```

Prints all keys and values for SomeTable in the specified MDBX database.