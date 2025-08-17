"""Data management tools for MCP Postgres server."""

import logging
import time
from typing import Any, Literal

from ..core.connection import connection_manager
from ..core.security import (
    check_table_access,
    sanitize_parameters,
    validate_query_permissions,
)
from ..utils.exceptions import (
    SecurityError,
    ValidationError,
    handle_postgres_error,
)
from ..utils.formatters import (
    format_error_response,
    format_success_response,
    serialize_value,
)
from ..utils.validators import validate_column_name, validate_table_name


logger = logging.getLogger(__name__)


async def insert_data(
    table_name: str,
    data: dict[str, Any],
    return_columns: list[str] | None = None,
    on_conflict: str = "error",
) -> dict[str, Any]:
    """Insert a single record into a table with validation and error handling.

    This tool safely inserts data into a PostgreSQL table with comprehensive
    validation, security checks, and flexible conflict resolution.

    Args:
        table_name: Name of the target table
        data: Dictionary of column names and values to insert
        return_columns: List of columns to return from inserted row (e.g., ['id', 'created_at'])
        on_conflict: How to handle conflicts ('error', 'ignore', 'update')

    Returns:
        Dictionary containing insertion results and metadata

    Raises:
        ValidationError: If table name, data, or parameters are invalid
        SecurityError: If table access is denied
        QueryExecutionError: If insertion fails
    """

    fetch_mode: Literal["all", "one", "val", "none"]

    try:
        # Validate inputs
        validate_table_name(table_name)

        if not data or not isinstance(data, dict):
            raise ValidationError("Data must be a non-empty dictionary")

        if on_conflict not in {"error", "ignore", "update"}:
            raise ValidationError("on_conflict must be one of: error, ignore, update")

        # Security validation
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        # Validate column names
        for column_name in data.keys():
            validate_column_name(column_name)

        # Prepare column names and values
        columns = list(data.keys())
        values = list(data.values())

        # Sanitize parameters
        clean_values = sanitize_parameters(values)

        # Build parameterized INSERT query
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        column_list = ", ".join(columns)

        base_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

        # Handle conflict resolution
        if on_conflict == "ignore":
            query = f"{base_query} ON CONFLICT DO NOTHING"
        elif on_conflict == "update":
            # Simple update strategy - update all non-key columns
            update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns]
            query = (
                f"{base_query} ON CONFLICT DO UPDATE SET {', '.join(update_clauses)}"
            )
        else:  # on_conflict == "error"
            query = base_query

        # Add RETURNING clause if requested
        if return_columns:
            for col in return_columns:
                validate_column_name(col)
            return_clause = ", ".join(return_columns)
            query += f" RETURNING {return_clause}"
            fetch_mode = "one"
        else:
            fetch_mode = "none"

        # Security validation of final query
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Record start time
        start_time = time.time()

        # Execute insertion
        logger.info(f"Inserting data into table: {table_name}")
        result = await connection_manager.execute_query(
            query=query, parameters=clean_values, fetch_mode=fetch_mode
        )

        execution_time = time.time() - start_time

        # Format response
        if return_columns and result:
            # Convert result to dictionary
            returned_data = dict(result) if hasattr(result, "keys") else {}
            response_data = {
                "inserted": True,
                "table_name": table_name,
                "returned_data": {
                    k: serialize_value(v) for k, v in returned_data.items()
                },
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "conflict_resolution": on_conflict,
                    "columns_inserted": len(columns),
                    "returned_columns": return_columns,
                },
            }
        else:
            # Parse status string for row count
            status_str = str(result) if result else "INSERT 0 0"
            rows_affected = (
                1 if "INSERT" in status_str and not status_str.endswith("0") else 0
            )

            response_data = {
                "inserted": rows_affected > 0,
                "table_name": table_name,
                "rows_affected": rows_affected,
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "conflict_resolution": on_conflict,
                    "columns_inserted": len(columns),
                    "status": status_str,
                },
            }

        logger.info(
            f"Data inserted successfully into {table_name} in {execution_time:.3f}s"
        )

        return format_success_response(
            data=response_data, message=f"Data inserted successfully into {table_name}"
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Insert validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Insert execution error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def update_data(
    table_name: str,
    data: dict[str, Any],
    where_conditions: dict[str, Any],
    return_columns: list[str] | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    """Update records in a table with conditional updates.

    This tool safely updates data in a PostgreSQL table with comprehensive
    validation, security checks, and flexible conditional logic.

    Args:
        table_name: Name of the target table
        data: Dictionary of column names and new values to update
        where_conditions: Dictionary of conditions for WHERE clause
        return_columns: List of columns to return from updated rows
        limit: Maximum number of rows to update (safety feature)

    Returns:
        Dictionary containing update results and metadata

    Raises:
        ValidationError: If table name, data, or parameters are invalid
        SecurityError: If table access is denied
        QueryExecutionError: If update fails
    """
    fetch_mode: Literal["all", "one", "val", "none"]
    try:
        # Validate inputs
        validate_table_name(table_name)

        if not data or not isinstance(data, dict):
            raise ValidationError("Data must be a non-empty dictionary")

        if not where_conditions or not isinstance(where_conditions, dict):
            raise ValidationError("Where conditions must be a non-empty dictionary")

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValidationError("Limit must be a positive integer")

        # Security validation
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        # Validate column names
        for column_name in list(data.keys()) + list(where_conditions.keys()):
            validate_column_name(column_name)

        # Prepare SET clause
        set_columns = list(data.keys())
        set_values = list(data.values())
        set_placeholders = [f"{col} = ${i + 1}" for i, col in enumerate(set_columns)]
        set_clause = ", ".join(set_placeholders)

        # Prepare WHERE clause
        where_columns = list(where_conditions.keys())
        where_values = list(where_conditions.values())
        where_start_idx = len(set_values)
        where_placeholders = [
            f"{col} = ${where_start_idx + i + 1}" for i, col in enumerate(where_columns)
        ]
        where_clause = " AND ".join(where_placeholders)

        # Combine all parameters
        all_values = set_values + where_values
        clean_values = sanitize_parameters(all_values)

        # Build UPDATE query
        query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        # Add LIMIT clause if specified (PostgreSQL doesn't support LIMIT in UPDATE directly)
        if limit is not None:
            # Use a subquery with LIMIT for safety
            primary_key_query = f"""
            UPDATE {table_name} SET {set_clause}
            WHERE ctid IN (
                SELECT ctid FROM {table_name} WHERE {where_clause} LIMIT {limit}
            )
            """
            query = primary_key_query

        # Add RETURNING clause if requested
        if return_columns:
            for col in return_columns:
                validate_column_name(col)
            return_clause = ", ".join(return_columns)
            query += f" RETURNING {return_clause}"
            fetch_mode = "all"
        else:
            fetch_mode = "none"

        # Security validation of final query
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Record start time
        start_time = time.time()

        # Execute update
        logger.info(f"Updating data in table: {table_name}")
        result = await connection_manager.execute_query(
            query=query, parameters=clean_values, fetch_mode=fetch_mode
        )

        execution_time = time.time() - start_time

        # Format response
        if return_columns and result:
            # Convert results to list of dictionaries
            returned_data = []
            if isinstance(result, list):
                for row in result:
                    row_dict = dict(row) if hasattr(row, "keys") else {}
                    returned_data.append(
                        {k: serialize_value(v) for k, v in row_dict.items()}
                    )

            response_data = {
                "updated": True,
                "table_name": table_name,
                "rows_affected": len(returned_data),
                "returned_data": returned_data,
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "columns_updated": len(set_columns),
                    "where_conditions": len(where_columns),
                    "returned_columns": return_columns,
                    "limit_applied": limit,
                },
            }
        else:
            # Parse status string for row count
            status_str = str(result) if result else "UPDATE 0"
            rows_affected = 0
            if "UPDATE" in status_str:
                try:
                    rows_affected = int(status_str.split()[-1])
                except (IndexError, ValueError):
                    rows_affected = 0

            response_data = {
                "updated": rows_affected > 0,
                "table_name": table_name,
                "rows_affected": rows_affected,
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "columns_updated": len(set_columns),
                    "where_conditions": len(where_columns),
                    "limit_applied": limit,
                    "status": status_str,
                },
            }

        logger.info(
            f"Data updated successfully in {table_name}: {response_data['rows_affected']} rows in {execution_time:.3f}s"
        )

        return format_success_response(
            data=response_data,
            message=f"Updated {response_data['rows_affected']} rows in {table_name}",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Update validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Update execution error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def delete_data(
    table_name: str,
    where_conditions: dict[str, Any],
    return_columns: list[str] | None = None,
    limit: int | None = None,
    confirm_delete: bool = False,
) -> dict[str, Any]:
    """Delete records from a table with safety confirmations.

    This tool safely deletes data from a PostgreSQL table with comprehensive
    validation, security checks, and mandatory safety confirmations.

    Args:
        table_name: Name of the target table
        where_conditions: Dictionary of conditions for WHERE clause
        return_columns: List of columns to return from deleted rows
        limit: Maximum number of rows to delete (safety feature)
        confirm_delete: Must be True to proceed with deletion (safety feature)

    Returns:
        Dictionary containing deletion results and metadata

    Raises:
        ValidationError: If table name, conditions, or parameters are invalid
        SecurityError: If table access is denied or confirmation not provided
        QueryExecutionError: If deletion fails
    """
    fetch_mode: Literal["all", "one", "val", "none"]
    try:
        # Validate inputs
        validate_table_name(table_name)

        if not where_conditions or not isinstance(where_conditions, dict):
            raise ValidationError("Where conditions must be a non-empty dictionary")

        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise ValidationError("Limit must be a positive integer")

        # Safety confirmation check
        if not confirm_delete:
            raise SecurityError(
                "Delete operation requires explicit confirmation. Set confirm_delete=True to proceed."
            )

        # Security validation
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        # Validate column names
        for column_name in where_conditions.keys():
            validate_column_name(column_name)

        if return_columns:
            for col in return_columns:
                validate_column_name(col)

        # Prepare WHERE clause
        where_columns = list(where_conditions.keys())
        where_values = list(where_conditions.values())
        where_placeholders = [
            f"{col} = ${i + 1}" for i, col in enumerate(where_columns)
        ]
        where_clause = " AND ".join(where_placeholders)

        # Sanitize parameters
        clean_values = sanitize_parameters(where_values)

        # Build DELETE query
        query = f"DELETE FROM {table_name} WHERE {where_clause}"

        # Add LIMIT clause if specified (PostgreSQL doesn't support LIMIT in DELETE directly)
        if limit is not None:
            # Use a subquery with LIMIT for safety
            limited_query = f"""
            DELETE FROM {table_name}
            WHERE ctid IN (
                SELECT ctid FROM {table_name} WHERE {where_clause} LIMIT {limit}
            )
            """
            query = limited_query

        # Add RETURNING clause if requested
        if return_columns:
            return_clause = ", ".join(return_columns)
            query += f" RETURNING {return_clause}"
            fetch_mode = "all"
        else:
            fetch_mode = "none"

        # Security validation of final query
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Record start time
        start_time = time.time()

        # Execute deletion
        logger.info(f"Deleting data from table: {table_name}")
        result = await connection_manager.execute_query(
            query=query, parameters=clean_values, fetch_mode=fetch_mode
        )

        execution_time = time.time() - start_time

        # Format response
        if return_columns and result:
            # Convert results to list of dictionaries
            returned_data = []
            if isinstance(result, list):
                for row in result:
                    row_dict = dict(row) if hasattr(row, "keys") else {}
                    returned_data.append(
                        {k: serialize_value(v) for k, v in row_dict.items()}
                    )

            response_data = {
                "deleted": True,
                "table_name": table_name,
                "rows_affected": len(returned_data),
                "returned_data": returned_data,
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "where_conditions": len(where_columns),
                    "returned_columns": return_columns,
                    "limit_applied": limit,
                    "confirmed": confirm_delete,
                },
            }
        else:
            # Parse status string for row count
            status_str = str(result) if result else "DELETE 0"
            rows_affected = 0
            if "DELETE" in status_str:
                try:
                    rows_affected = int(status_str.split()[-1])
                except (IndexError, ValueError):
                    rows_affected = 0

            response_data = {
                "deleted": rows_affected > 0,
                "table_name": table_name,
                "rows_affected": rows_affected,
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {
                    "where_conditions": len(where_columns),
                    "limit_applied": limit,
                    "confirmed": confirm_delete,
                    "status": status_str,
                },
            }

        logger.info(
            f"Data deleted successfully from {table_name}: {response_data['rows_affected']} rows in {execution_time:.3f}s"
        )

        return format_success_response(
            data=response_data,
            message=f"Deleted {response_data['rows_affected']} rows from {table_name}",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Delete validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Delete execution error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def bulk_insert(
    table_name: str,
    data: list[dict[str, Any]],
    batch_size: int = 1000,
    on_conflict: str = "error",
    return_summary: bool = True,
) -> dict[str, Any]:
    """Efficiently insert large datasets with batch processing.

    This tool handles bulk insertion of data into PostgreSQL tables with
    optimized batch processing, conflict resolution, and progress tracking.

    Args:
        table_name: Name of the target table
        data: List of dictionaries containing records to insert
        batch_size: Number of records to process in each batch (default: 1000)
        on_conflict: How to handle conflicts ('error', 'ignore', 'update')
        return_summary: Whether to return detailed summary statistics

    Returns:
        Dictionary containing bulk insertion results and metadata

    Raises:
        ValidationError: If table name, data, or parameters are invalid
        SecurityError: If table access is denied
        QueryExecutionError: If bulk insertion fails
    """
    try:
        # Validate inputs
        validate_table_name(table_name)

        if not data or not isinstance(data, list):
            raise ValidationError("Data must be a non-empty list of dictionaries")

        if not all(isinstance(record, dict) for record in data):
            raise ValidationError("All data items must be dictionaries")

        if batch_size <= 0 or batch_size > 10000:
            raise ValidationError("Batch size must be between 1 and 10000")

        if on_conflict not in {"error", "ignore", "update"}:
            raise ValidationError("on_conflict must be one of: error, ignore, update")

        # Security validation
        if not check_table_access(table_name):
            raise SecurityError(f"Access denied to table: {table_name}")

        # Validate data consistency
        if not data:
            raise ValidationError("Data list cannot be empty")

        # Get column names from first record
        first_record = data[0]
        if not first_record:
            raise ValidationError("First record cannot be empty")

        columns = list(first_record.keys())

        # Validate all column names
        for column_name in columns:
            validate_column_name(column_name)

        # Validate that all records have the same columns
        for i, record in enumerate(data):
            if set(record.keys()) != set(columns):
                raise ValidationError(
                    f"Record {i} has different columns than first record. "
                    f"Expected: {columns}, Got: {list(record.keys())}"
                )

        # Record start time
        start_time = time.time()

        # Initialize counters
        total_records = len(data)
        processed_records = 0
        successful_batches = 0
        failed_batches = 0
        batch_errors = []

        logger.info(
            f"Starting bulk insert of {total_records} records into {table_name} with batch size {batch_size}"
        )

        # Process data in batches
        for batch_start in range(0, total_records, batch_size):
            batch_end = min(batch_start + batch_size, total_records)
            batch_data = data[batch_start:batch_end]
            batch_num = (batch_start // batch_size) + 1

            try:
                # Prepare batch values
                batch_values = []
                for record in batch_data:
                    record_values = [record[col] for col in columns]
                    batch_values.extend(record_values)

                # Sanitize all parameters
                clean_values = sanitize_parameters(batch_values)

                # Build multi-row INSERT query
                values_per_record = len(columns)
                value_groups = []

                for i in range(len(batch_data)):
                    start_idx = i * values_per_record
                    placeholders = [
                        f"${start_idx + j + 1}" for j in range(values_per_record)
                    ]
                    value_groups.append(f"({', '.join(placeholders)})")

                column_list = ", ".join(columns)
                values_clause = ", ".join(value_groups)

                base_query = (
                    f"INSERT INTO {table_name} ({column_list}) VALUES {values_clause}"
                )

                # Handle conflict resolution
                if on_conflict == "ignore":
                    query = f"{base_query} ON CONFLICT DO NOTHING"
                elif on_conflict == "update":
                    update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns]
                    query = f"{base_query} ON CONFLICT DO UPDATE SET {', '.join(update_clauses)}"
                else:  # on_conflict == "error"
                    query = base_query

                # Security validation of batch query
                is_valid, error_msg = validate_query_permissions(query)
                if not is_valid:
                    raise SecurityError(
                        f"Batch query security validation failed: {error_msg}"
                    )

                # Execute batch
                await connection_manager.execute_query(
                    query=query, parameters=clean_values, fetch_mode="none"
                )

                processed_records += len(batch_data)
                successful_batches += 1

                logger.debug(f"Batch {batch_num} completed: {len(batch_data)} records")

            except Exception as batch_error:
                failed_batches += 1
                error_info = {
                    "batch_number": batch_num,
                    "batch_start": batch_start,
                    "batch_size": len(batch_data),
                    "error": str(batch_error),
                }
                batch_errors.append(error_info)

                logger.error(f"Batch {batch_num} failed: {batch_error}")

                # For error mode, stop on first failure
                if on_conflict == "error":
                    raise ValidationError(
                        f"Bulk insert failed at batch {batch_num}: {batch_error}"
                    ) from batch_error

        execution_time = time.time() - start_time

        # Calculate success rate
        success_rate = (
            (successful_batches / (successful_batches + failed_batches)) * 100
            if (successful_batches + failed_batches) > 0
            else 0
        )

        # Format response
        response_data = {
            "bulk_inserted": successful_batches > 0,
            "table_name": table_name,
            "total_records": total_records,
            "processed_records": processed_records,
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "success_rate_percent": round(success_rate, 2),
            "execution_time_ms": round(execution_time * 1000, 2),
            "metadata": {
                "batch_size": batch_size,
                "conflict_resolution": on_conflict,
                "columns_inserted": len(columns),
                "batches_total": successful_batches + failed_batches,
                "avg_batch_time_ms": round(
                    (execution_time * 1000)
                    / max(successful_batches + failed_batches, 1),
                    2,
                ),
            },
        }

        # Add detailed summary if requested
        if return_summary:
            response_data["summary"] = {
                "columns": columns,
                "records_per_batch": batch_size,
                "processing_rate_per_sec": round(
                    processed_records / max(execution_time, 0.001), 2
                ),
            }

            if batch_errors:
                response_data["errors"] = batch_errors[:10]  # Limit error details

        logger.info(
            f"Bulk insert completed for {table_name}: "
            f"{processed_records}/{total_records} records in {execution_time:.3f}s "
            f"({successful_batches} successful, {failed_batches} failed batches)"
        )

        return format_success_response(
            data=response_data,
            message=f"Bulk inserted {processed_records} records into {table_name}",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Bulk insert validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Bulk insert execution error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


# Tool schema definitions for MCP registration
INSERT_DATA_SCHEMA = {
    "name": "insert_data",
    "description": "Insert new records into a table with validation and error handling",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to insert data into",
            },
            "data": {
                "type": "array",
                "description": "Array of records to insert, each record is an object with column names as keys",
                "items": {"type": "object"},
                "minItems": 1,
            },
            "on_conflict": {
                "type": "string",
                "description": "How to handle conflicts",
                "enum": ["error", "ignore", "update"],
                "default": "error",
            },
            "return_records": {
                "type": "boolean",
                "description": "Whether to return the inserted records",
                "default": False,
            },
        },
        "required": ["table_name", "data"],
    },
}

UPDATE_DATA_SCHEMA = {
    "name": "update_data",
    "description": "Update existing records in a table based on specified conditions",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to update",
            },
            "set_values": {
                "type": "object",
                "description": "Object with column names as keys and new values",
            },
            "where_conditions": {
                "type": "object",
                "description": "Object with column names as keys and condition values",
            },
            "return_records": {
                "type": "boolean",
                "description": "Whether to return the updated records",
                "default": False,
            },
        },
        "required": ["table_name", "set_values", "where_conditions"],
    },
}

DELETE_DATA_SCHEMA = {
    "name": "delete_data",
    "description": "Delete records from a table based on specified conditions with safety confirmations",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to delete from",
            },
            "where_conditions": {
                "type": "object",
                "description": "Object with column names as keys and condition values",
            },
            "confirm_delete": {
                "type": "boolean",
                "description": "Confirmation flag to prevent accidental deletions",
                "default": False,
            },
            "return_count": {
                "type": "boolean",
                "description": "Whether to return the count of deleted records",
                "default": True,
            },
        },
        "required": ["table_name", "where_conditions", "confirm_delete"],
    },
}

BULK_INSERT_SCHEMA = {
    "name": "bulk_insert",
    "description": "Insert large datasets efficiently using bulk operations with progress tracking",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to insert data into",
            },
            "data": {
                "type": "array",
                "description": "Array of records to insert in bulk",
                "items": {"type": "object"},
                "minItems": 1,
            },
            "batch_size": {
                "type": "integer",
                "description": "Number of records to insert per batch",
                "default": 1000,
                "minimum": 1,
                "maximum": 10000,
            },
            "on_conflict": {
                "type": "string",
                "description": "How to handle conflicts during bulk insert",
                "enum": ["error", "ignore", "update"],
                "default": "error",
            },
        },
        "required": ["table_name", "data"],
    },
}

# Export tool functions and schemas
__all__ = [
    "insert_data",
    "update_data",
    "delete_data",
    "bulk_insert",
    "INSERT_DATA_SCHEMA",
    "UPDATE_DATA_SCHEMA",
    "DELETE_DATA_SCHEMA",
    "BULK_INSERT_SCHEMA",
]
