# MCP PostgreSQL Tools Documentation

This document provides comprehensive documentation for all 50+ tools available in the MCP PostgreSQL server. The tools are organized into 10 functional modules, each serving specific database interaction needs.

## Table of Contents

- [Overview](#overview)
- [Query Tools (3 tools)](#query-tools)
- [Schema Tools (8 tools)](#schema-tools)
- [Analysis Tools (4 tools)](#analysis-tools)
- [Data Tools (4 tools)](#data-tools)
- [Relation Tools (3 tools)](#relation-tools)
- [Performance Tools (3 tools)](#performance-tools)
- [Backup Tools (3 tools)](#backup-tools)
- [Admin Tools (4 tools)](#admin-tools)
- [Validation Tools (3 tools)](#validation-tools)
- [Generation Tools (3 tools)](#generation-tools)
- [Usage Examples](#usage-examples)

## Overview

The MCP PostgreSQL server provides 50+ specialized tools for comprehensive database interaction through the Model Context Protocol. All tools use parameterized queries to prevent SQL injection and include comprehensive error handling and validation.

### Tool Categories

| Module | Tool Count | Purpose |
|--------|------------|---------|
| Query Tools | 3 | SQL query execution and transactions |
| Schema Tools | 8 | Database schema inspection and metadata |
| Analysis Tools | 4 | Data analysis and profiling |
| Data Tools | 4 | CRUD operations and data management |
| Relation Tools | 3 | Foreign key and relationship analysis |
| Performance Tools | 3 | Query optimization and performance monitoring |
| Backup Tools | 3 | Data export, import, and backup operations |
| Admin Tools | 4 | Database administration and maintenance |
| Validation Tools | 3 | Data integrity and constraint validation |
| Generation Tools | 3 | Code and SQL template generation |

---

## Query Tools

Tools for executing SQL queries and managing transactions safely.

### `execute_query`

Execute a parameterized SQL query safely with parameter binding to prevent SQL injection.

**Parameters:**
- `query` (string, required): SQL query with parameter placeholders ($1, $2, etc.)
- `parameters` (array, optional): List of parameters to bind to the query placeholders
- `fetch_mode` (string, optional): How to fetch results
  - `"all"` (default): Return all rows
  - `"one"`: Return single row or null
  - `"none"`: Return status only (for INSERT/UPDATE/DELETE)
  - `"val"`: Return single value from first row/column

**Example:**
```json
{
  "query": "SELECT * FROM users WHERE age > $1 AND city = $2",
  "parameters": [25, "New York"],
  "fetch_mode": "all"
}
```

**Returns:** Query results with execution metadata including row count and execution time.

---

### `execute_raw_query`

Execute a raw SQL query without parameter binding (WARNING: potential SQL injection risk).

**Parameters:**
- `query` (string, required): Raw SQL query to execute (ensure input is trusted)
- `fetch_mode` (string, optional): How to fetch results (same options as `execute_query`)

**Example:**
```json
{
  "query": "SELECT COUNT(*) FROM products WHERE category = 'electronics'",
  "fetch_mode": "val"
}
```

**Returns:** Query results with security warning about SQL injection risk.

---

### `execute_transaction`

Execute multiple queries in a single transaction with automatic rollback on failure.

**Parameters:**
- `queries` (array, required): List of query objects to execute in transaction
  - Each query object contains:
    - `query` (string, required): SQL query with parameter placeholders
    - `parameters` (array, optional): Parameters for the query
    - `fetch_mode` (string, optional): How to fetch results for this query

**Example:**
```json
{
  "queries": [
    {
      "query": "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
      "parameters": [123, 99.99],
      "fetch_mode": "none"
    },
    {
      "query": "UPDATE inventory SET quantity = quantity - $1 WHERE product_id = $2",
      "parameters": [1, 456],
      "fetch_mode": "none"
    }
  ]
}
```

**Returns:** Results from all queries with transaction metadata and rollback information.

---

## Schema Tools

Tools for inspecting database schema including tables, indexes, constraints, views, functions, triggers, and sequences.

### `list_tables`

List all tables in the database with metadata including size, type, and row estimates.

**Parameters:**
- `schema_name` (string, optional): Schema name to filter tables (defaults to 'public')

**Example:**
```json
{
  "schema_name": "public"
}
```

**Returns:** List of tables with metadata including size, type, estimated rows, and comments.

---

### `describe_table`

Get detailed information about a specific table structure including columns, types, and constraints.

**Parameters:**
- `table_name` (string, required): Name of the table to describe
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "table_name": "users",
  "schema_name": "public"
}
```

**Returns:** Detailed table structure with column information, data types, constraints, and relationships.

---

### `list_indexes`

List indexes with performance and usage information including size and scan statistics.

**Parameters:**
- `table_name` (string, optional): Table name to filter indexes
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "table_name": "users",
  "schema_name": "public"
}
```

**Returns:** Index information with performance statistics, size, and usage patterns.

---

### `list_constraints`

List table constraints including foreign keys, check constraints, and unique constraints.

**Parameters:**
- `table_name` (string, optional): Table name to filter constraints
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "table_name": "orders",
  "schema_name": "public"
}
```

**Returns:** Constraint information including type, columns, and referential actions.

---

### `list_views`

List database views with their definitions and dependencies.

**Parameters:**
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "schema_name": "public"
}
```

**Returns:** View definitions, dependencies, and updatability information.

---

### `list_functions`

List stored procedures and functions with their signatures and properties.

**Parameters:**
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "schema_name": "public"
}
```

**Returns:** Function information including parameters, return types, and language.

---

### `list_triggers`

List database triggers with their definitions and properties.

**Parameters:**
- `table_name` (string, optional): Table name to filter triggers
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "table_name": "audit_log",
  "schema_name": "public"
}
```

**Returns:** Trigger definitions, events, timing, and associated functions.

---

### `list_sequences`

List database sequences with their current values and properties.

**Parameters:**
- `schema_name` (string, optional): Schema name (defaults to 'public')

**Example:**
```json
{
  "schema_name": "public"
}
```

**Returns:** Sequence information including current values, increment, and ownership.

---

## Analysis Tools

Tools for data analysis and profiling to understand data patterns and quality.

### `analyze_column`

Perform statistical analysis on a specific column including count, nulls, distinct values, and distribution.

**Parameters:**
- `table_name` (string, required): Name of the table containing the column
- `column_name` (string, required): Name of the column to analyze

**Example:**
```json
{
  "table_name": "sales",
  "column_name": "amount"
}
```

**Returns:** Statistical analysis including basic stats, numeric/text statistics, and frequent values.

---

### `find_duplicates`

Find duplicate records in a table based on specified columns or all columns.

**Parameters:**
- `table_name` (string, required): Name of the table to check for duplicates
- `columns` (array, optional): List of column names to check for duplicates
- `limit` (integer, optional): Maximum number of duplicate groups to return (default: 100)

**Example:**
```json
{
  "table_name": "customers",
  "columns": ["email", "phone"],
  "limit": 50
}
```

**Returns:** Duplicate groups with counts and row identifiers.

---

### `profile_table`

Analyze data distribution and types across all columns in a table with comprehensive profiling.

**Parameters:**
- `table_name` (string, required): Name of the table to profile
- `sample_size` (integer, optional): Sample size for large tables (uses TABLESAMPLE)

**Example:**
```json
{
  "table_name": "products",
  "sample_size": 10000
}
```

**Returns:** Comprehensive table profile with column statistics, data types, and distribution analysis.

---

### `analyze_correlations`

Analyze correlations between numeric columns in a table to identify relationships.

**Parameters:**
- `table_name` (string, required): Name of the table to analyze
- `columns` (array, optional): List of numeric column names to analyze
- `method` (string, optional): Correlation method ('pearson' only supported currently)

**Example:**
```json
{
  "table_name": "sales_data",
  "columns": ["price", "quantity", "discount"],
  "method": "pearson"
}
```

**Returns:** Correlation matrix and relationship analysis between numeric columns.

---

## Data Tools

Tools for data manipulation and management including CRUD operations.

### `insert_data`

Insert new records into a table with validation and error handling.

**Parameters:**
- `table_name` (string, required): Target table name
- `data` (object, required): Dictionary of column-value pairs to insert
- `return_columns` (array, optional): List of columns to return from inserted row
- `on_conflict` (string, optional): How to handle conflicts ('error', 'ignore', 'update')

**Example:**
```json
{
  "table_name": "users",
  "data": {
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  },
  "return_columns": ["id", "created_at"],
  "on_conflict": "ignore"
}
```

**Returns:** Insert status with returned column values and metadata.

---

### `update_data`

Update existing records in a table based on specified conditions.

**Parameters:**
- `table_name` (string, required): Target table name
- `data` (object, required): Dictionary of column-value pairs to update
- `where_conditions` (object, required): Conditions to identify records to update
- `return_columns` (array, optional): Columns to return after update
- `limit` (integer, optional): Maximum number of records to update

**Example:**
```json
{
  "table_name": "products",
  "data": {
    "price": 29.99,
    "updated_at": "2024-01-15T10:30:00Z"
  },
  "where_conditions": {
    "category": "electronics",
    "status": "active"
  },
  "limit": 100
}
```

**Returns:** Update status with affected row count and returned values.

---

### `delete_data`

Delete records from a table based on specified conditions with safety confirmations.

**Parameters:**
- `table_name` (string, required): Target table name
- `where_conditions` (object, required): Conditions to identify records to delete
- `limit` (integer, optional): Maximum number of records to delete
- `confirm_delete` (boolean, optional): Confirmation flag for safety

**Example:**
```json
{
  "table_name": "temp_data",
  "where_conditions": {
    "created_at": "< '2024-01-01'",
    "status": "processed"
  },
  "limit": 1000,
  "confirm_delete": true
}
```

**Returns:** Delete status with affected row count and safety confirmations.

---

### `bulk_insert`

Insert large datasets efficiently using bulk operations with progress tracking.

**Parameters:**
- `table_name` (string, required): Target table name
- `data` (array, required): Array of objects representing records to insert
- `batch_size` (integer, optional): Number of records per batch (default: 1000)
- `on_conflict` (string, optional): How to handle conflicts ('error', 'ignore', 'update')

**Example:**
```json
{
  "table_name": "sales_records",
  "data": [
    {"product_id": 1, "quantity": 5, "price": 99.99},
    {"product_id": 2, "quantity": 3, "price": 149.99}
  ],
  "batch_size": 500,
  "on_conflict": "ignore"
}
```

**Returns:** Bulk insert status with progress tracking and error handling.

---

## Relation Tools

Tools for exploring table relationships and foreign key constraints.

### `get_foreign_keys`

Get foreign key relationships for a specific table including referenced tables and columns.

**Parameters:**
- `table_name` (string, required): Name of the table to analyze
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `direction` (string, optional): Relationship direction ('outgoing', 'incoming', 'both')

**Example:**
```json
{
  "table_name": "orders",
  "schema_name": "public",
  "direction": "both"
}
```

**Returns:** Foreign key relationships with referenced tables, columns, and constraint actions.

---

### `get_table_relationships`

Get comprehensive table relationships including parent-child connections and dependency mapping.

**Parameters:**
- `table_name` (string, optional): Specific table to analyze relationships for
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `max_depth` (integer, optional): Maximum relationship depth to traverse

**Example:**
```json
{
  "table_name": "users",
  "schema_name": "public",
  "max_depth": 3
}
```

**Returns:** Comprehensive relationship mapping with dependency hierarchy.

---

### `validate_referential_integrity`

Validate referential integrity by checking for constraint violations and orphaned records.

**Parameters:**
- `table_name` (string, optional): Specific table to validate
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `constraint_name` (string, optional): Specific constraint to validate

**Example:**
```json
{
  "table_name": "order_items",
  "schema_name": "public"
}
```

**Returns:** Referential integrity validation results with violation details.

---

## Performance Tools

Tools for database performance monitoring and optimization.

### `analyze_query_performance`

Analyze query performance and provide execution plan analysis with EXPLAIN ANALYZE.

**Parameters:**
- `query` (string, required): SQL query to analyze
- `parameters` (array, optional): Query parameters for parameterized queries
- `analyze_mode` (string, optional): Analysis mode ('explain', 'analyze', 'buffers')

**Example:**
```json
{
  "query": "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > $1",
  "parameters": [1000],
  "analyze_mode": "analyze"
}
```

**Returns:** Query execution plan with performance metrics and optimization suggestions.

---

### `find_slow_queries`

Find slow queries from PostgreSQL statistics to identify performance bottlenecks.

**Parameters:**
- `min_duration_ms` (number, optional): Minimum query duration in milliseconds
- `limit` (integer, optional): Maximum number of queries to return
- `order_by` (string, optional): Sort order ('duration', 'calls', 'mean_time')

**Example:**
```json
{
  "min_duration_ms": 1000,
  "limit": 20,
  "order_by": "duration"
}
```

**Returns:** List of slow queries with performance statistics and execution details.

---

### `get_table_stats`

Get comprehensive table statistics including storage usage and access patterns.

**Parameters:**
- `table_name` (string, optional): Specific table to analyze
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `include_indexes` (boolean, optional): Include index statistics

**Example:**
```json
{
  "table_name": "products",
  "schema_name": "public",
  "include_indexes": true
}
```

**Returns:** Table statistics including size, access patterns, and index usage.

---

## Backup Tools

Tools for data export, import, and backup operations.

### `export_table_csv`

Export table data to CSV format with customizable options.

**Parameters:**
- `table_name` (string, required): Name of the table to export
- `file_path` (string, optional): Output file path (if not provided, returns CSV content)
- `query` (string, optional): Custom query to filter exported data
- `delimiter` (string, optional): CSV delimiter character (default: ',')
- `include_headers` (boolean, optional): Include column headers (default: true)
- `limit` (integer, optional): Maximum number of rows to export

**Example:**
```json
{
  "table_name": "sales_data",
  "file_path": "/tmp/sales_export.csv",
  "query": "SELECT * FROM sales_data WHERE date >= '2024-01-01'",
  "delimiter": ",",
  "include_headers": true,
  "limit": 10000
}
```

**Returns:** Export status with file information or CSV content.

---

### `import_csv_data`

Import CSV data into a PostgreSQL table with validation and conflict handling.

**Parameters:**
- `table_name` (string, required): Target table name
- `file_path` (string, optional): Path to CSV file (if not provided, expects csv_content)
- `csv_content` (string, optional): CSV content as string
- `delimiter` (string, optional): CSV delimiter character (default: ',')
- `has_header` (boolean, optional): Whether CSV has header row (default: true)
- `on_conflict` (string, optional): How to handle conflicts ('error', 'ignore', 'update')
- `batch_size` (integer, optional): Number of rows per batch (default: 1000)

**Example:**
```json
{
  "table_name": "imported_data",
  "file_path": "/tmp/data.csv",
  "delimiter": ",",
  "has_header": true,
  "on_conflict": "ignore",
  "batch_size": 500
}
```

**Returns:** Import status with progress tracking and error handling.

---

### `backup_table`

Create a complete backup of a table including structure and/or data.

**Parameters:**
- `table_name` (string, required): Name of the table to backup
- `backup_name` (string, optional): Name for the backup (auto-generated if not provided)
- `include_data` (boolean, optional): Include table data in backup (default: true)
- `include_structure` (boolean, optional): Include table structure in backup (default: true)
- `compression` (string, optional): Compression method ('none', 'gzip')

**Example:**
```json
{
  "table_name": "critical_data",
  "backup_name": "critical_data_backup_2024_01_15",
  "include_data": true,
  "include_structure": true,
  "compression": "gzip"
}
```

**Returns:** Backup status with backup file information and metadata.

---

## Admin Tools

Tools for database administration and maintenance operations.

### `get_database_info`

Get comprehensive database metadata including version, size, connections, and statistics.

**Parameters:** None

**Example:**
```json
{}
```

**Returns:** Database information including version, size, connection limits, and system statistics.

---

### `monitor_connections`

Monitor active database connections, sessions, and identify long-running or blocked queries.

**Parameters:**
- `include_idle` (boolean, optional): Include idle connections (default: false)
- `min_duration_seconds` (number, optional): Minimum query duration to include

**Example:**
```json
{
  "include_idle": false,
  "min_duration_seconds": 30
}
```

**Returns:** Active connection information with query details and blocking relationships.

---

### `vacuum_table`

Perform VACUUM operation on a table to reclaim storage space and update statistics.

**Parameters:**
- `table_name` (string, required): Name of the table to vacuum
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `full` (boolean, optional): Perform VACUUM FULL (default: false)
- `analyze` (boolean, optional): Also run ANALYZE (default: true)
- `verbose` (boolean, optional): Verbose output (default: false)

**Example:**
```json
{
  "table_name": "large_table",
  "schema_name": "public",
  "full": false,
  "analyze": true,
  "verbose": true
}
```

**Returns:** VACUUM operation results with space reclaimed and statistics.

---

### `reindex_table`

Rebuild indexes for a table or specific index to improve performance and reclaim space.

**Parameters:**
- `table_name` (string, required): Name of the table to reindex
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `index_name` (string, optional): Specific index to rebuild
- `concurrently` (boolean, optional): Perform REINDEX CONCURRENTLY (default: false)

**Example:**
```json
{
  "table_name": "products",
  "schema_name": "public",
  "index_name": "idx_products_category",
  "concurrently": true
}
```

**Returns:** Reindex operation results with performance improvements.

---

## Validation Tools

Tools for data integrity and constraint validation.

### `validate_constraints`

Validate table constraints and identify any constraint violations.

**Parameters:**
- `table_name` (string, optional): Specific table to validate
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `constraint_type` (string, optional): Type of constraint to validate ('CHECK', 'FOREIGN KEY', 'UNIQUE', 'PRIMARY KEY')

**Example:**
```json
{
  "table_name": "orders",
  "schema_name": "public",
  "constraint_type": "FOREIGN KEY"
}
```

**Returns:** Constraint validation results with violation details and affected rows.

---

### `validate_data_types`

Validate data type compliance and identify type conversion issues.

**Parameters:**
- `table_name` (string, required): Name of the table to validate
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `column_name` (string, optional): Specific column to validate

**Example:**
```json
{
  "table_name": "user_data",
  "schema_name": "public",
  "column_name": "email"
}
```

**Returns:** Data type validation results with conversion issues and recommendations.

---

### `check_data_integrity`

Perform comprehensive data integrity checks including constraints, types, and relationships.

**Parameters:**
- `table_name` (string, optional): Specific table to check
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `check_types` (array, optional): Types of checks to perform ('constraints', 'types', 'relationships', 'duplicates')

**Example:**
```json
{
  "table_name": "customer_orders",
  "schema_name": "public",
  "check_types": ["constraints", "relationships", "duplicates"]
}
```

**Returns:** Comprehensive integrity check results with issues and recommendations.

---

## Generation Tools

Tools for code and SQL template generation.

### `generate_ddl`

Generate CREATE TABLE DDL statement for an existing table with columns, constraints, and indexes.

**Parameters:**
- `table_name` (string, required): Name of the table
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `include_indexes` (boolean, optional): Include CREATE INDEX statements (default: true)
- `include_constraints` (boolean, optional): Include constraint definitions (default: true)
- `include_comments` (boolean, optional): Include table and column comments (default: true)

**Example:**
```json
{
  "table_name": "users",
  "schema_name": "public",
  "include_indexes": true,
  "include_constraints": true,
  "include_comments": true
}
```

**Returns:** Complete DDL statements for table recreation.

---

### `generate_insert_template`

Generate INSERT statement templates for a table with parameter placeholders and sample data.

**Parameters:**
- `table_name` (string, required): Name of the table
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `include_optional` (boolean, optional): Include nullable columns (default: true)
- `template_type` (string, optional): Template format ('parameterized', 'values', 'both')

**Example:**
```json
{
  "table_name": "products",
  "schema_name": "public",
  "include_optional": true,
  "template_type": "both"
}
```

**Returns:** INSERT statement templates with parameter placeholders and sample values.

---

### `generate_orm_model`

Generate ORM model class definition for a table (SQLAlchemy, Django, or Pydantic).

**Parameters:**
- `table_name` (string, required): Name of the table
- `schema_name` (string, optional): Schema name (defaults to 'public')
- `model_type` (string, required): Type of ORM model ('sqlalchemy', 'django', 'pydantic')
- `class_name` (string, optional): Custom class name for the model
- `include_relationships` (boolean, optional): Include foreign key relationships (default: true)

**Example:**
```json
{
  "table_name": "users",
  "schema_name": "public",
  "model_type": "sqlalchemy",
  "class_name": "User",
  "include_relationships": true
}
```

**Returns:** Generated ORM model class definition with proper imports and relationships.

---

## Usage Examples

### Basic Query Operations

```json
// Execute a simple SELECT query
{
  "tool": "execute_query",
  "parameters": {
    "query": "SELECT name, email FROM users WHERE active = $1",
    "parameters": [true],
    "fetch_mode": "all"
  }
}

// Insert new record
{
  "tool": "insert_data",
  "parameters": {
    "table_name": "users",
    "data": {
      "name": "Alice Smith",
      "email": "alice@example.com",
      "active": true
    },
    "return_columns": ["id", "created_at"]
  }
}
```

### Schema Exploration

```json
// List all tables
{
  "tool": "list_tables",
  "parameters": {
    "schema_name": "public"
  }
}

// Get detailed table structure
{
  "tool": "describe_table",
  "parameters": {
    "table_name": "orders",
    "schema_name": "public"
  }
}
```

### Data Analysis

```json
// Analyze column statistics
{
  "tool": "analyze_column",
  "parameters": {
    "table_name": "sales",
    "column_name": "revenue"
  }
}

// Find duplicate records
{
  "tool": "find_duplicates",
  "parameters": {
    "table_name": "customers",
    "columns": ["email"],
    "limit": 50
  }
}
```

### Performance Monitoring

```json
// Analyze query performance
{
  "tool": "analyze_query_performance",
  "parameters": {
    "query": "SELECT * FROM orders WHERE total > $1",
    "parameters": [1000],
    "analyze_mode": "analyze"
  }
}

// Find slow queries
{
  "tool": "find_slow_queries",
  "parameters": {
    "min_duration_ms": 5000,
    "limit": 10,
    "order_by": "duration"
  }
}
```

### Data Export/Import

```json
// Export table to CSV
{
  "tool": "export_table_csv",
  "parameters": {
    "table_name": "products",
    "file_path": "/tmp/products.csv",
    "include_headers": true,
    "limit": 10000
  }
}

// Import CSV data
{
  "tool": "import_csv_data",
  "parameters": {
    "table_name": "imported_products",
    "file_path": "/tmp/products.csv",
    "has_header": true,
    "on_conflict": "ignore"
  }
}
```

### Code Generation

```json
// Generate SQLAlchemy model
{
  "tool": "generate_orm_model",
  "parameters": {
    "table_name": "users",
    "model_type": "sqlalchemy",
    "class_name": "User",
    "include_relationships": true
  }
}

// Generate DDL statements
{
  "tool": "generate_ddl",
  "parameters": {
    "table_name": "products",
    "include_indexes": true,
    "include_constraints": true
  }
}
```

---

## Error Handling

All tools include comprehensive error handling and return structured error responses:

- **Validation Errors**: Invalid parameters or table/column names
- **Security Errors**: Access denied or security validation failures
- **Query Execution Errors**: SQL syntax errors or constraint violations
- **Connection Errors**: Database connectivity issues

Error responses include error codes, descriptive messages, and context information to help diagnose and resolve issues.

## Security Features

- **SQL Injection Prevention**: All queries use parameterized statements
- **Access Control**: Table-level access validation
- **Input Sanitization**: Comprehensive parameter validation
- **Query Pattern Validation**: Whitelist approach for allowed operations
- **Audit Logging**: Structured logging of all tool usage

## Performance Considerations

- **Connection Pooling**: Efficient async connection management
- **Query Optimization**: Execution plan analysis and recommendations
- **Batch Operations**: Bulk insert and update capabilities
- **Result Limiting**: Configurable limits to prevent resource exhaustion
- **Caching**: Schema metadata caching for improved performance
