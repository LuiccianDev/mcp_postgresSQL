# MCP PostgreSQL Tools Documentation

## Table of Contents
- [Analysis Tools](#analysis-tools)
- [Backup Tools](#backup-tools)
- [Data Tools](#data-tools)
- [Generation Tools](#generation-tools)
- [Performance Tools](#performance-tools)
- [Query Tools](#query-tools)
- [Relation Tools](#relation-tools)
- [Schema Tools](#schema-tools)
- [Validation Tools](#validation-tools)

## Analysis Tools

Tools for data analysis and profiling.

### `analyze_column`
Perform statistical analysis on a specific column.

**Parameters:**
- `table_name` (str): Name of the table containing the column
- `column_name` (str): Name of the column to analyze

**Returns:**
Dictionary containing statistical analysis results including count, nulls, distinct values, min/max values, and data distribution.

### `analyze_correlations`
Analyze correlations between numeric columns in a table.

**Parameters:**
- `table_name` (str): Name of the table to analyze
- `columns` (list[str], optional): List of numeric column names to analyze
- `method` (str): Correlation method ('pearson' only supported currently)

**Returns:**
Dictionary containing correlation analysis results.

### `find_duplicates`
Find duplicate records in a table based on specified columns.

**Parameters:**
- `table_name` (str): Name of the table to analyze
- `columns` (list[str]): List of column names to check for duplicates

**Returns:**
Dictionary containing information about duplicate records.

## Backup Tools

Tools for database backup and restore operations.

### `backup_table`
Create a backup of a table.

**Parameters:**
- `table_name` (str): Name of the table to back up
- `backup_name` (str): Name for the backup

**Returns:**
Dictionary with backup status and details.

### `export_table_csv`
Export table data to a CSV file.

**Parameters:**
- `table_name` (str): Name of the table to export
- `file_path` (str): Path to save the CSV file
- `query` (str, optional): Optional query to filter exported data
- `delimiter` (str): CSV delimiter character

**Returns:**
Dictionary with export status and details.

### `import_csv_data`
Import data from a CSV file into a table.

**Parameters:**
- `table_name` (str): Target table name
- `file_path` (str): Path to the CSV file
- `delimiter` (str): CSV delimiter character
- `header` (bool): Whether the CSV has a header row

**Returns:**
Dictionary with import status and details.

## Data Tools

Tools for data manipulation and management.

### `insert_data`
Insert data into a table.

**Parameters:**
- `table_name` (str): Target table name
- `data` (dict): Dictionary of column-value pairs to insert

**Returns:**
Dictionary with insert status and details.

### `update_data`
Update records in a table.

**Parameters:**
- `table_name` (str): Target table name
- `data` (dict): Dictionary of column-value pairs to update
- `where_conditions` (dict): Conditions to identify records to update
- `return_columns` (list[str], optional): Columns to return after update
- `limit` (int, optional): Maximum number of records to update

**Returns:**
Dictionary with update status and details.

### `delete_data`
Delete records from a table.

**Parameters:**
- `table_name` (str): Target table name
- `where_conditions` (dict): Conditions to identify records to delete

**Returns:**
Dictionary with delete status and details.

## Generation Tools

Tools for code and SQL generation.

### `generate_orm_model`
Generate ORM model class definition for a table.

**Parameters:**
- `table_name` (str): Name of the table
- `schema_name` (str, optional): Schema name (defaults to 'public')
- `model_type` (str): Type of ORM model ('sqlalchemy', 'django', 'pydantic')
- `class_name` (str, optional): Custom class name for the model

**Returns:**
Dictionary containing the generated model code.

### `generate_insert_template`
Generate INSERT statement template for a table.

**Parameters:**
- `table_name` (str): Name of the table
- `schema_name` (str, optional): Schema name (defaults to 'public')
- `include_optional` (bool): Whether to include nullable columns

**Returns:**
Dictionary containing the generated INSERT template.

## Performance Tools

Tools for database performance monitoring and optimization.

### `monitor_connections`
Monitor active database connections.

**Returns:**
Dictionary with connection information.

### `get_database_info`
Get general database information.

**Returns:**
Dictionary with database statistics and configuration.

## Query Tools

Tools for executing and analyzing SQL queries.

### `execute_query`
Execute a SQL query with parameters.

**Parameters:**
- `query` (str): SQL query to execute
- `params` (list, optional): Query parameters

**Returns:**
Query results.

## Schema Tools

Tools for database schema inspection and manipulation.

### `list_tables`
List all tables in a schema.

**Parameters:**
- `schema_name` (str, optional): Schema name (defaults to 'public')

**Returns:**
List of tables in the schema.

### `describe_table`
Get detailed information about a table.

**Parameters:**
- `table_name` (str): Name of the table
- `schema_name` (str, optional): Schema name (defaults to 'public')

**Returns:**
Detailed table information including columns, constraints, and indexes.

## Validation Tools

Tools for data validation and integrity checking.

### `validate_constraints`
Validate table constraints.

**Parameters:**
- `table_name` (str): Name of the table to validate
- `constraint_name` (str, optional): Specific constraint to validate

**Returns:**
Validation results.

### `validate_data_types`
Validate column data types.

**Parameters:**
- `table_name` (str): Name of the table
- `column_name` (str, optional): Specific column to validate

**Returns:**
Data type validation results.

### `check_data_integrity`
Check data integrity for a table.

**Parameters:**
- `table_name` (str): Name of the table to check

**Returns:**
Data integrity check results.
