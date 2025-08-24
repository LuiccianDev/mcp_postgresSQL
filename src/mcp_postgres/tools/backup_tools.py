"""Backup and restore tools for MCP Postgres server."""

import csv
import io
import logging
import time
from typing import Any

from mcp_postgres.core.connection import connection_manager
from mcp_postgres.core.security import (
    check_table_access,
    sanitize_parameters,
    validate_query_permissions,
)
from mcp_postgres.utils.exceptions import (
    SecurityError,
    TableNotFoundError,
    ValidationError,
    handle_postgres_error,
)
from mcp_postgres.utils.formatters import (
    format_error_response,
    format_success_response,
    serialize_value,
)
from mcp_postgres.utils.validators import validate_table_name


logger = logging.getLogger(__name__)


async def export_table_csv(
    table_name: str,
    columns: list[str] | None = None,
    where_clause: str | None = None,
    parameters: list[Any] | None = None,
    include_headers: bool = True,
    delimiter: str = ",",
    quote_char: str = '"',
    limit: int | None = None,
) -> dict[str, Any]:
    """Export table data to CSV format.

    This tool exports data from a PostgreSQL table to CSV format with customizable
    options for columns, filtering, and CSV formatting.

    Args:
        table_name: Name of the table to export
        columns: List of column names to export (None for all columns)
        where_clause: Optional WHERE clause for filtering (without WHERE keyword)
        parameters: Parameters for the WHERE clause
        include_headers: Whether to include column headers in CSV
        delimiter: CSV field delimiter (default: comma)
        quote_char: CSV quote character (default: double quote)
        limit: Maximum number of rows to export (None for all rows)

    Returns:
        Dictionary containing CSV data and export metadata

    Raises:
        ValidationError: If table name or parameters are invalid
        SecurityError: If table access is denied
        TableNotFoundError: If table doesn't exist
    """
    try:
        # Validate inputs
        if not validate_table_name(table_name):
            raise ValidationError(f"Invalid table name: {table_name}")

        # Check table access permissions
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        # Validate delimiter and quote_char
        if len(delimiter) != 1:
            raise ValidationError("Delimiter must be a single character")
        if len(quote_char) != 1:
            raise ValidationError("Quote character must be a single character")

        # Validate limit
        if limit is not None and limit <= 0:
            raise ValidationError("Limit must be a positive integer")

        # Build column list
        if columns:
            # Validate column names
            for col in columns:
                if not col or not isinstance(col, str):
                    raise ValidationError(f"Invalid column name: {col}")
            column_list = ", ".join(f'"{col}"' for col in columns)
        else:
            column_list = "*"

        # Build query
        query = f'SELECT {column_list} FROM "{table_name}"'

        # Add WHERE clause if provided
        query_params = []
        if where_clause:
            # Validate WHERE clause security
            full_where_query = f"SELECT 1 WHERE {where_clause}"
            is_valid, error_msg = validate_query_permissions(full_where_query)
            if not is_valid:
                raise SecurityError(
                    f"WHERE clause security validation failed: {error_msg}"
                )

            query += f" WHERE {where_clause}"
            query_params = sanitize_parameters(parameters or [])

        # Add LIMIT if specified
        if limit:
            query += f" LIMIT {limit}"

        # Record start time
        start_time = time.time()

        # Execute query
        logger.info(f"Exporting table {table_name} to CSV")
        result = await connection_manager.execute_query(
            query=query, parameters=query_params, fetch_mode="all"
        )

        execution_time = time.time() - start_time

        if not result:
            return format_success_response(
                data={
                    "csv_data": "",
                    "row_count": 0,
                    "column_count": 0,
                    "columns": [],
                    "export_time_ms": round(execution_time * 1000, 2),
                    "metadata": {
                        "table_name": table_name,
                        "include_headers": include_headers,
                        "delimiter": delimiter,
                        "quote_char": quote_char,
                        "has_where_clause": where_clause is not None,
                        "has_limit": limit is not None,
                    },
                },
                message="Table exported successfully (no data found)",
            )

        # Get column names from first row
        column_names = list(result[0].keys())

        # Create CSV data
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(
            csv_buffer,
            delimiter=delimiter,
            quotechar=quote_char,
            quoting=csv.QUOTE_MINIMAL,
        )

        # Write headers if requested
        if include_headers:
            csv_writer.writerow(column_names)

        # Write data rows
        for row in result:
            # Convert values to strings, handling special types
            csv_row = []
            for col_name in column_names:
                value = row[col_name]
                csv_row.append(str(serialize_value(value)) if value is not None else "")
            csv_writer.writerow(csv_row)

        csv_data = csv_buffer.getvalue()
        csv_buffer.close()

        logger.info(
            f"CSV export completed for table {table_name}: {len(result)} rows in {execution_time:.3f}s"
        )

        return format_success_response(
            data={
                "csv_data": csv_data,
                "row_count": len(result),
                "column_count": len(column_names),
                "columns": column_names,
                "export_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "table_name": table_name,
                    "include_headers": include_headers,
                    "delimiter": delimiter,
                    "quote_char": quote_char,
                    "has_where_clause": where_clause is not None,
                    "has_limit": limit is not None,
                    "exported_columns": columns or "all",
                },
            },
            message=f"Table {table_name} exported successfully to CSV",
        )

    except (ValidationError, SecurityError, TableNotFoundError) as e:
        logger.warning(f"CSV export validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"CSV export error: {e}")
        mcp_error = handle_postgres_error(
            e,
            query if "query" in locals() else None,
            query_params if "query_params" in locals() else None,
        )
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def import_csv_data(
    table_name: str,
    csv_data: str,
    has_headers: bool = True,
    delimiter: str = ",",
    quote_char: str = '"',
    columns: list[str] | None = None,
    validate_data: bool = True,
    on_conflict: str = "error",
    batch_size: int = 1000,
) -> dict[str, Any]:
    """Import CSV data into a PostgreSQL table.

    This tool imports CSV data into a table with validation and conflict handling.
    It supports batch processing for large datasets.

    Args:
        table_name: Name of the target table
        csv_data: CSV data as a string
        has_headers: Whether CSV data includes column headers
        delimiter: CSV field delimiter (default: comma)
        quote_char: CSV quote character (default: double quote)
        columns: Column names if CSV doesn't have headers (required if has_headers=False)
        validate_data: Whether to validate data types before insertion
        on_conflict: How to handle conflicts ('error', 'skip', 'update')
        batch_size: Number of rows to insert per batch

    Returns:
        Dictionary containing import results and statistics

    Raises:
        ValidationError: If CSV data or parameters are invalid
        SecurityError: If table access is denied
        TableNotFoundError: If table doesn't exist
    """
    try:
        # Validate inputs
        if not validate_table_name(table_name):
            raise ValidationError(f"Invalid table name: {table_name}")

        # Check table access permissions
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        if not csv_data or not csv_data.strip():
            raise ValidationError("CSV data cannot be empty")

        # Validate delimiter and quote_char
        if len(delimiter) != 1:
            raise ValidationError("Delimiter must be a single character")
        if len(quote_char) != 1:
            raise ValidationError("Quote character must be a single character")

        # Validate on_conflict option
        if on_conflict not in {"error", "skip", "update"}:
            raise ValidationError("on_conflict must be one of: error, skip, update")

        # Validate batch_size
        if batch_size <= 0:
            raise ValidationError("batch_size must be a positive integer")

        # Record start time
        start_time = time.time()

        # Parse CSV data
        csv_buffer = io.StringIO(csv_data)
        csv_reader = csv.reader(csv_buffer, delimiter=delimiter, quotechar=quote_char)

        rows = list(csv_reader)
        csv_buffer.close()

        if not rows:
            raise ValidationError("CSV data contains no rows")

        # Handle headers
        if has_headers:
            if len(rows) < 2:
                raise ValidationError(
                    "CSV with headers must have at least 2 rows (header + data)"
                )
            header_row = rows[0]
            data_rows = rows[1:]
            column_names = [col.strip() for col in header_row]
        else:
            if not columns:
                raise ValidationError(
                    "columns parameter is required when has_headers=False"
                )
            column_names = columns
            data_rows = rows

        # Validate column names
        for col in column_names:
            if not col or not isinstance(col, str):
                raise ValidationError(f"Invalid column name: {col}")

        # Get table schema for validation
        schema_query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = $1
        ORDER BY ordinal_position
        """

        table_schema = await connection_manager.execute_query(
            query=schema_query, parameters=[table_name], fetch_mode="all"
        )

        if not table_schema:
            raise TableNotFoundError(f"Table {table_name} not found")

        # Create column mapping
        schema_columns = {row["column_name"]: row for row in table_schema}

        # Validate that all CSV columns exist in table
        for col_name in column_names:
            if col_name not in schema_columns:
                raise ValidationError(
                    f"Column {col_name} does not exist in table {table_name}"
                )

        # Prepare insert query
        placeholders = ", ".join(f"${i + 1}" for i in range(len(column_names)))
        quoted_columns = ", ".join(f'"{col}"' for col in column_names)

        if on_conflict == "skip":
            insert_query = f"""
            INSERT INTO "{table_name}" ({quoted_columns})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
            """
        elif on_conflict == "update":
            # For update, we need a unique constraint - this is simplified
            # In practice, you'd need to specify which columns to update
            update_clause = ", ".join(
                f'"{col}" = EXCLUDED."{col}"' for col in column_names
            )
            insert_query = f"""
            INSERT INTO "{table_name}" ({quoted_columns})
            VALUES ({placeholders})
            ON CONFLICT DO UPDATE SET {update_clause}
            """
        else:  # error
            insert_query = f"""
            INSERT INTO "{table_name}" ({quoted_columns})
            VALUES ({placeholders})
            """

        # Process data in batches
        total_rows = len(data_rows)
        processed_rows = 0
        successful_rows = 0
        failed_rows = 0
        errors = []

        logger.info(
            f"Starting CSV import for table {table_name}: {total_rows} rows in batches of {batch_size}"
        )

        for batch_start in range(0, total_rows, batch_size):
            batch_end = min(batch_start + batch_size, total_rows)
            batch_rows = data_rows[batch_start:batch_end]

            # Prepare batch data
            batch_queries = []
            for row_idx, row in enumerate(batch_rows):
                if len(row) != len(column_names):
                    error_msg = f"Row {batch_start + row_idx + 1}: Expected {len(column_names)} columns, got {len(row)}"
                    errors.append(error_msg)
                    failed_rows += 1
                    continue

                # Convert and validate data types if requested
                processed_row: list[Any] | None = []
                if validate_data:
                    for _col_idx, (col_name, value) in enumerate(
                        zip(column_names, row, strict=False)
                    ):
                        if processed_row is None:
                            break  # La fila ya es inválida, no seguimos
                        try:
                            schema_col = schema_columns[col_name]
                            data_type = schema_col["data_type"]

                            if (
                                value.strip() == ""
                                and schema_col["is_nullable"] == "YES"
                            ):
                                processed_row.append(None)
                            elif data_type in ("integer", "bigint", "smallint"):
                                processed_row.append(int(value))
                            elif data_type in (
                                "numeric",
                                "decimal",
                                "real",
                                "double precision",
                            ):
                                processed_row.append(float(value))
                            elif data_type == "boolean":
                                processed_row.append(
                                    value.lower() in ("true", "t", "1", "yes", "y")
                                )
                            else:
                                processed_row.append(value)
                        except (ValueError, TypeError) as e:
                            error_msg = f"Row {batch_start + row_idx + 1}, Column {col_name}: Invalid value '{value}' for type {data_type}: {e}"
                            errors.append(error_msg)
                            processed_row = None  # Marca la fila como inválida
                            break
                else:
                    processed_row = list(row)

                if processed_row is not None:
                    batch_queries.append(
                        {
                            "query": insert_query,
                            "parameters": processed_row,
                            "fetch_mode": "none",
                        }
                    )

            # Execute batch
            if batch_queries:
                try:
                    await connection_manager.execute_transaction(batch_queries)
                    successful_rows += len(batch_queries)
                    logger.debug(
                        f"Batch {batch_start // batch_size + 1}: {len(batch_queries)} rows inserted successfully"
                    )
                except Exception as e:
                    logger.error(f"Batch {batch_start // batch_size + 1} failed: {e}")
                    failed_rows += len(batch_queries)
                    errors.append(
                        f"Batch {batch_start // batch_size + 1} failed: {str(e)}"
                    )

            processed_rows += len(batch_rows)

        execution_time = time.time() - start_time

        logger.info(
            f"CSV import completed for table {table_name}: {successful_rows}/{total_rows} rows imported in {execution_time:.3f}s"
        )

        return format_success_response(
            data={
                "total_rows": total_rows,
                "successful_rows": successful_rows,
                "failed_rows": failed_rows,
                "processed_rows": processed_rows,
                "import_time_ms": round(execution_time * 1000, 2),
                "errors": errors[:10],  # Limit errors to first 10
                "error_count": len(errors),
                "metadata": {
                    "table_name": table_name,
                    "columns": column_names,
                    "column_count": len(column_names),
                    "has_headers": has_headers,
                    "delimiter": delimiter,
                    "quote_char": quote_char,
                    "validate_data": validate_data,
                    "on_conflict": on_conflict,
                    "batch_size": batch_size,
                },
            },
            message=f"CSV import completed: {successful_rows}/{total_rows} rows imported successfully",
        )

    except (ValidationError, SecurityError, TableNotFoundError) as e:
        logger.warning(f"CSV import validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"CSV import error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def backup_table(
    table_name: str,
    include_data: bool = True,
    include_structure: bool = True,
    where_clause: str | None = None,
    parameters: list[Any] | None = None,
    format_type: str = "sql",
) -> dict[str, Any]:
    """Create a complete backup of a table including structure and/or data.

    This tool creates a comprehensive backup that can be used to recreate the table
    with its structure, data, indexes, and constraints.

    Args:
        table_name: Name of the table to backup
        include_data: Whether to include table data in backup
        include_structure: Whether to include table structure (DDL) in backup
        where_clause: Optional WHERE clause for filtering data (without WHERE keyword)
        parameters: Parameters for the WHERE clause
        format_type: Output format ('sql' for SQL statements, 'json' for structured data)

    Returns:
        Dictionary containing backup data and metadata

    Raises:
        ValidationError: If table name or parameters are invalid
        SecurityError: If table access is denied
        TableNotFoundError: If table doesn't exist
    """
    try:
        # Validate inputs
        if not validate_table_name(table_name):
            raise ValidationError(f"Invalid table name: {table_name}")

        # Check table access permissions
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        if not include_data and not include_structure:
            raise ValidationError(
                "At least one of include_data or include_structure must be True"
            )

        if format_type not in {"sql", "json"}:
            raise ValidationError("format_type must be 'sql' or 'json'")

        # Record start time
        start_time = time.time()

        backup_data = {
            "table_name": table_name,
            "backup_timestamp": time.time(),
            "structure": None,
            "data": None,
            "metadata": {},
        }

        logger.info(f"Starting backup for table {table_name}")

        # Get table structure if requested
        if include_structure:
            # Get table definition
            table_def_query = """
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_name = $1
            ORDER BY ordinal_position
            """

            columns_info = await connection_manager.execute_query(
                query=table_def_query, parameters=[table_name], fetch_mode="all"
            )

            if not columns_info:
                raise TableNotFoundError(f"Table {table_name} not found")

            # Get primary key information
            pk_query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_name = $1
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            """

            pk_columns = await connection_manager.execute_query(
                query=pk_query, parameters=[table_name], fetch_mode="all"
            )

            # Get foreign key information
            fk_query = """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.table_name = $1
                AND tc.constraint_type = 'FOREIGN KEY'
            """

            fk_constraints = await connection_manager.execute_query(
                query=fk_query, parameters=[table_name], fetch_mode="all"
            )

            # Get indexes
            index_query = """
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE tablename = $1
                AND indexname NOT LIKE '%_pkey'
            """

            indexes = await connection_manager.execute_query(
                query=index_query, parameters=[table_name], fetch_mode="all"
            )

            if format_type == "sql":
                # Generate CREATE TABLE statement
                column_definitions = []
                for col in columns_info:
                    col_def = f'"{col["column_name"]}" {col["data_type"]}'

                    # Add length/precision
                    if col["character_maximum_length"]:
                        col_def += f"({col['character_maximum_length']})"
                    elif col["numeric_precision"]:
                        if col["numeric_scale"]:
                            col_def += (
                                f"({col['numeric_precision']},{col['numeric_scale']})"
                            )
                        else:
                            col_def += f"({col['numeric_precision']})"

                    # Add NOT NULL
                    if col["is_nullable"] == "NO":
                        col_def += " NOT NULL"

                    # Add DEFAULT
                    if col["column_default"]:
                        col_def += f" DEFAULT {col['column_default']}"

                    column_definitions.append(col_def)

                # Add primary key
                if pk_columns:
                    pk_cols = ", ".join(f'"{col["column_name"]}"' for col in pk_columns)
                    column_definitions.append(f"PRIMARY KEY ({pk_cols})")

                # Add foreign keys
                for fk in fk_constraints:
                    fk_def = f'CONSTRAINT "{fk["constraint_name"]}" FOREIGN KEY ("{fk["column_name"]}") REFERENCES "{fk["foreign_table_name"]}"("{fk["foreign_column_name"]}")'
                    column_definitions.append(fk_def)

                create_table_sql = (
                    f'CREATE TABLE "{table_name}" (\n  '
                    + ",\n  ".join(column_definitions)
                    + "\n);"
                )

                # Add indexes
                index_statements = []
                for idx in indexes:
                    index_statements.append(idx["indexdef"] + ";")

                structure_sql = create_table_sql
                if index_statements:
                    structure_sql += "\n\n-- Indexes\n" + "\n".join(index_statements)

                backup_data["structure"] = structure_sql

            else:  # json format
                backup_data["structure"] = {
                    "columns": [dict(col) for col in columns_info],
                    "primary_key": [dict(col) for col in pk_columns],
                    "foreign_keys": [dict(fk) for fk in fk_constraints],
                    "indexes": [dict(idx) for idx in indexes],
                }

        # Get table data if requested
        if include_data:
            # Build data query
            data_query = f'SELECT * FROM "{table_name}"'
            query_params = []

            if where_clause:
                # Validate WHERE clause security
                full_where_query = f"SELECT 1 WHERE {where_clause}"
                is_valid, error_msg = validate_query_permissions(full_where_query)
                if not is_valid:
                    raise SecurityError(
                        f"WHERE clause security validation failed: {error_msg}"
                    )

                data_query += f" WHERE {where_clause}"
                query_params = sanitize_parameters(parameters or [])

            # Execute data query
            table_data = await connection_manager.execute_query(
                query=data_query, parameters=query_params, fetch_mode="all"
            )

            if format_type == "sql":
                # Generate INSERT statements
                if table_data:
                    column_names = list(table_data[0].keys())
                    quoted_columns = ", ".join(f'"{col}"' for col in column_names)

                    insert_statements = []
                    for row in table_data:
                        values = []
                        for col_name in column_names:
                            value = row[col_name]
                            if value is None:
                                values.append("NULL")
                            elif isinstance(value, str):
                                # Escape single quotes
                                escaped_value = value.replace("'", "''")
                                values.append(f"'{escaped_value}'")
                            elif isinstance(value, bool):
                                values.append("TRUE" if value else "FALSE")
                            else:
                                values.append(str(serialize_value(value)))

                        values_str = ", ".join(values)
                        insert_statements.append(
                            f'INSERT INTO "{table_name}" ({quoted_columns}) VALUES ({values_str});'
                        )

                    backup_data["data"] = "\n".join(insert_statements)
                else:
                    backup_data["data"] = f"-- No data found in table {table_name}"

            else:  # json format
                backup_data["data"] = (
                    [dict(row) for row in table_data] if table_data else []
                )

        execution_time = time.time() - start_time

        # Add metadata
        backup_data["metadata"] = {
            "backup_time_ms": round(execution_time * 1000, 2),
            "include_data": include_data,
            "include_structure": include_structure,
            "format_type": format_type,
            "has_where_clause": where_clause is not None,
            "data_row_count": len(table_data)
            if include_data and "table_data" in locals()
            else 0,
            "structure_included": include_structure,
        }

        logger.info(f"Table backup completed for {table_name} in {execution_time:.3f}s")

        return format_success_response(
            data=backup_data,
            message=f"Table {table_name} backup completed successfully",
        )

    except (ValidationError, SecurityError, TableNotFoundError) as e:
        logger.warning(f"Table backup validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Table backup error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


# Tool schema definitions for MCP registration
EXPORT_TABLE_CSV_SCHEMA = {
    "name": "export_table_csv",
    "description": "Export table data to CSV format with customizable options",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to export",
            },
            "columns": {
                "type": "array",
                "description": "List of column names to export (null for all columns)",
                "items": {"type": "string"},
                "default": None,
            },
            "where_clause": {
                "type": "string",
                "description": "Optional WHERE clause for filtering (without WHERE keyword)",
                "default": None,
            },
            "parameters": {
                "type": "array",
                "description": "Parameters for the WHERE clause",
                "items": {"type": ["string", "number", "boolean", "null"]},
                "default": [],
            },
            "include_headers": {
                "type": "boolean",
                "description": "Whether to include column headers in CSV",
                "default": True,
            },
            "delimiter": {
                "type": "string",
                "description": "CSV field delimiter",
                "default": ",",
                "maxLength": 1,
            },
            "quote_char": {
                "type": "string",
                "description": "CSV quote character",
                "default": '"',
                "maxLength": 1,
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of rows to export (null for all rows)",
                "minimum": 1,
                "default": None,
            },
        },
        "required": ["table_name"],
    },
}

IMPORT_CSV_DATA_SCHEMA = {
    "name": "import_csv_data",
    "description": "Import CSV data into a PostgreSQL table with validation and conflict handling",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the target table",
            },
            "csv_data": {
                "type": "string",
                "description": "CSV data as a string",
            },
            "has_headers": {
                "type": "boolean",
                "description": "Whether CSV data includes column headers",
                "default": True,
            },
            "delimiter": {
                "type": "string",
                "description": "CSV field delimiter",
                "default": ",",
                "maxLength": 1,
            },
            "quote_char": {
                "type": "string",
                "description": "CSV quote character",
                "default": '"',
                "maxLength": 1,
            },
            "columns": {
                "type": "array",
                "description": "Column names if CSV doesn't have headers (required if has_headers=False)",
                "items": {"type": "string"},
                "default": None,
            },
            "validate_data": {
                "type": "boolean",
                "description": "Whether to validate data types before insertion",
                "default": True,
            },
            "on_conflict": {
                "type": "string",
                "description": "How to handle conflicts",
                "enum": ["error", "skip", "update"],
                "default": "error",
            },
            "batch_size": {
                "type": "integer",
                "description": "Number of rows to insert per batch",
                "minimum": 1,
                "maximum": 10000,
                "default": 1000,
            },
        },
        "required": ["table_name", "csv_data"],
    },
}

BACKUP_TABLE_SCHEMA = {
    "name": "backup_table",
    "description": "Create a complete backup of a table including structure and/or data",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to backup",
            },
            "include_data": {
                "type": "boolean",
                "description": "Whether to include table data in backup",
                "default": True,
            },
            "include_structure": {
                "type": "boolean",
                "description": "Whether to include table structure (DDL) in backup",
                "default": True,
            },
            "where_clause": {
                "type": "string",
                "description": "Optional WHERE clause for filtering data (without WHERE keyword)",
                "default": None,
            },
            "parameters": {
                "type": "array",
                "description": "Parameters for the WHERE clause",
                "items": {"type": ["string", "number", "boolean", "null"]},
                "default": [],
            },
            "format_type": {
                "type": "string",
                "description": "Output format",
                "enum": ["sql", "json"],
                "default": "sql",
            },
        },
        "required": ["table_name"],
    },
}

# Export tool functions and schemas
__all__ = [
    "export_table_csv",
    "import_csv_data",
    "backup_table",
    "EXPORT_TABLE_CSV_SCHEMA",
    "IMPORT_CSV_DATA_SCHEMA",
    "BACKUP_TABLE_SCHEMA",
]
