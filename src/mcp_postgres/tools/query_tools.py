"""Query execution tools for MCP Postgres server."""

import logging
import time
from typing import Any, Literal

from ..core.connection import connection_manager
from ..core.security import sanitize_parameters, validate_query_permissions
from ..utils.exceptions import (
    SecurityError,
    ValidationError,
    handle_postgres_error,
)
from ..utils.formatters import (
    format_error_response,
    format_query_result,
    format_success_response,
    serialize_value,
)


logger = logging.getLogger(__name__)


async def execute_query(
    query: str,
    parameters: list[Any] | None = None,
    fetch_mode: Literal["all", "one", "none", "val"] = "all"
) -> dict[str, Any]:
    """Execute a parameterized SQL query safely.

    This tool executes SQL queries using parameter binding to prevent SQL injection.
    It supports various fetch modes for different query types.

    Args:
        query: SQL query with parameter placeholders ($1, $2, etc.)
        parameters: List of parameters to bind to the query
        fetch_mode: How to fetch results ('all', 'one', 'none', 'val')
            - 'all': Return all rows (default for SELECT)
            - 'one': Return single row or None
            - 'none': Return status string (for INSERT/UPDATE/DELETE)
            - 'val': Return single value from first row/column

    Returns:
        Dictionary containing query results and metadata

    Raises:
        ValidationError: If query or parameters are invalid
        SecurityError: If query fails security validation
        QueryExecutionError: If query execution fails
    """
    try:
        # Validate inputs
        if not query or not query.strip():
            raise ValidationError("Query cannot be empty")

        if fetch_mode not in {"all", "one", "none", "val"}:
            raise ValidationError(
                f"Invalid fetch_mode: {fetch_mode}. Must be one of: all, one, none, val"
            )

        # Security validation
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Sanitize parameters
        clean_parameters = sanitize_parameters(parameters or [])

        # Record start time for performance metrics
        start_time = time.time()

        # Execute query
        result: Any
        if fetch_mode == "all":
            result = await connection_manager.execute_query(query, clean_parameters, "all")
        elif fetch_mode == "one":
            result = await connection_manager.execute_query(query, clean_parameters, "one")
        elif fetch_mode == "val":
            result = await connection_manager.execute_query(query, clean_parameters, "val")
        else:  # fetch_mode == "none"
            result = await connection_manager.execute_query(query, clean_parameters, "none")

        execution_time = time.time() - start_time

        # Format response based on fetch mode
        if fetch_mode == "all":
            # Convert asyncpg Records to dictionaries
            rows = []
            columns = []
            if result and isinstance(result, list):
                for row in result:
                    # Handle both real asyncpg Records and mock objects
                    if hasattr(row, "_asdict"):
                        rows.append(row._asdict())
                    elif hasattr(row, "items"):
                        rows.append(dict(row))
                    else:
                        rows.append({"value": row})
                columns = list(result[0].keys()) if result and hasattr(result[0], 'keys') else []

            formatted_result = format_query_result(
                rows=rows, columns=columns, execution_time=execution_time
            )

        elif fetch_mode == "one":
            if result is not None and hasattr(result, 'keys'):
                row_dict = dict(result)
                columns = list(result.keys())
                formatted_result = format_query_result(
                    rows=[row_dict], columns=columns, execution_time=execution_time
                )
            else:
                formatted_result = format_query_result(
                    rows=[], columns=[], execution_time=execution_time
                )

        elif fetch_mode == "val":
            value = result[0] if isinstance(result, list) and len(result) > 0 else result
            formatted_result = {
                "value": serialize_value(value),
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {"fetch_mode": "val", "has_value": value is not None},
            }

        else:  # fetch_mode == "none"
            # For INSERT/UPDATE/DELETE, result is a status string like "INSERT 0 1"
            formatted_result = {
                "status": str(result),
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {"fetch_mode": "none", "operation_completed": True},
            }

        logger.info(
            f"Query executed successfully in {execution_time:.3f}s, fetch_mode: {fetch_mode}"
        )

        return format_success_response(
            data=formatted_result, message="Query executed successfully"
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Query validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Query execution error: {e}")
        mcp_error = handle_postgres_error(e, query, parameters)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def execute_raw_query(query: str, fetch_mode: Literal["all", "one", "none", "val"] = "all") -> dict[str, Any]:
    """Execute a raw SQL query without parameter binding.

    WARNING: This tool executes raw SQL without parameter binding and may be
    vulnerable to SQL injection if used with untrusted input. Use execute_query
    with parameter binding whenever possible.

    Args:
        query: Raw SQL query to execute
        fetch_mode: How to fetch results ('all', 'one', 'none', 'val')

    Returns:
        Dictionary containing query results, metadata, and security warnings

    Raises:
        ValidationError: If query is invalid
        SecurityError: If query fails security validation
        QueryExecutionError: If query execution fails
    """
    try:
        # Validate inputs
        if not query or not query.strip():
            raise ValidationError("Query cannot be empty")

        if fetch_mode not in {"all", "one", "none", "val"}:
            raise ValidationError(
                f"Invalid fetch_mode: {fetch_mode}. Must be one of: all, one, none, val"
            )

        # Security validation (still applies to raw queries)
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Record start time for performance metrics
        start_time = time.time()

        # Execute raw query with warning
        logger.warning(
            f"Executing raw query (SQL injection risk): {query[:100]}{'...' if len(query) > 100 else ''}"
        )

        result = await connection_manager.execute_raw_query(
            query=query, fetch_mode=fetch_mode
        )

        execution_time = time.time() - start_time

        # Format response based on fetch mode (same logic as execute_query)
        if fetch_mode == "all":
            rows = []
            columns = []
            if result and isinstance(result, list):
                for row in result:
                    # Handle both real asyncpg Records and mock objects
                    if hasattr(row, "_asdict"):
                        rows.append(row._asdict())
                    elif hasattr(row, "items"):
                        rows.append(dict(row))
                    else:
                        rows.append({"value": row})
                columns = list(result[0].keys()) if result and hasattr(result[0], 'keys') else []

            formatted_result = format_query_result(
                rows=rows, columns=columns, execution_time=execution_time
            )

        elif fetch_mode == "one":
            if result is not None and hasattr(result, '__iter__') and not isinstance(result, str | bytes):
                if hasattr(result, "_asdict"):
                    row_dict = result._asdict()
                elif hasattr(result, "items"):
                    row_dict = dict(result)
                else:
                    row_dict = {"value": result}
                columns = list(row_dict.keys())
                formatted_result = format_query_result(
                    rows=[row_dict], columns=columns, execution_time=execution_time
                )
            else:
                formatted_result = format_query_result(
                    rows=[], columns=[], execution_time=execution_time
                )

        elif fetch_mode == "val":
            value = result[0] if isinstance(result, list) and len(result) > 0 else result
            formatted_result = {
                "value": serialize_value(value),
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {"fetch_mode": "val", "has_value": value is not None},
            }

        else:  # fetch_mode == "none"
            formatted_result = {
                "status": str(result),
                "execution_time_ms": round(execution_time * 1000, 2),
                "metadata": {"fetch_mode": "none", "operation_completed": True},
            }

        # Add security warning to response
        formatted_result["security_warning"] = (
            "This query was executed without parameter binding. "
            "Ensure input is trusted to prevent SQL injection attacks."
        )

        logger.info(
            f"Raw query executed successfully in {execution_time:.3f}s, fetch_mode: {fetch_mode}"
        )

        return format_success_response(
            data=formatted_result,
            message="Raw query executed successfully (with security warning)",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Raw query validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Raw query execution error: {e}")
        mcp_error = handle_postgres_error(e, query, None)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def execute_transaction(
    queries: list[dict[str, Any]]
) -> dict[str, Any]:
    """Execute multiple queries in a single transaction with error handling.

    Args:
        queries: List of query dictionaries with 'query', 'parameters', and 'fetch_mode'

    Returns:
        Dict[str, Any]: Normalized transaction results

    Raises:
        ValidationError, SecurityError
    """

    try:
        if not queries:
            raise ValidationError("Queries list cannot be empty")

        prepared_queries: list[dict[str, Any]] = []

        for i, q in enumerate(queries):
            if not isinstance(q, dict):
                raise ValidationError(f"Query {i} must be a dict")
            query = q.get("query")
            if not query or not query.strip():
                raise ValidationError(f"Query {i} missing 'query'")
            parameters = q.get("parameters", [])
            fetch_mode = q.get("fetch_mode", "all")
            if fetch_mode not in {"all", "one", "val", "none"}:
                raise ValidationError(f"Query {i} has invalid fetch_mode: {fetch_mode}")

            # Security validation & sanitize parameters
            is_valid, error_msg = validate_query_permissions(query)
            if not is_valid:
                raise SecurityError(f"Query {i} failed security: {error_msg}")
            clean_parameters = sanitize_parameters(parameters)

            prepared_queries.append(
                {"query": query, "parameters": clean_parameters, "fetch_mode": fetch_mode}
            )

        # Execute transaction
        start_time = time.time()
        logger.info(f"Starting transaction with {len(prepared_queries)} queries")

        results_raw = await connection_manager.execute_transaction(prepared_queries)
        execution_time = time.time() - start_time

        # Normalize results
        formatted_results: list[dict[str, Any]] = []

        for i, (query_info, result) in enumerate(zip(prepared_queries, results_raw, strict=False)):
            fetch_mode = query_info["fetch_mode"]

            try:
                if fetch_mode == "all":
                    rows: list[dict[str, Any]] = []
                    columns: list[str] = []

                    if result:
                        if isinstance(result, list):
                            for row in result:
                                if hasattr(row, "_asdict"):
                                    rows.append(row._asdict())
                                elif isinstance(row, dict):
                                    rows.append(row)
                                else:
                                    rows.append({"value": row})
                            columns = list(rows[0].keys()) if rows else []
                        else:
                            if hasattr(result, "_asdict"):
                                row_dict = result._asdict()
                            elif isinstance(result, dict):
                                row_dict = result
                            else:
                                row_dict = {"value": result}
                            rows.append(row_dict)
                            columns = list(row_dict.keys())

                    query_result = {
                        "query_index": i,
                        "rows": rows,
                        "columns": columns,
                        "row_count": len(rows),
                    }

                elif fetch_mode == "one":
                    if result:
                        if hasattr(result, "_asdict"):
                            row_dict = result._asdict()
                        elif isinstance(result, dict):
                            row_dict = result
                        else:
                            row_dict = {"value": result}
                        columns = list(row_dict.keys())
                        query_result = {
                            "query_index": i,
                            "rows": [row_dict],
                            "columns": columns,
                            "row_count": 1,
                        }
                    else:
                        query_result = {"query_index": i, "rows": [], "columns": [], "row_count": 0}

                elif fetch_mode == "val":
                    query_result = {
                        "query_index": i,
                        "value": serialize_value(result),
                        "has_value": result is not None,
                    }

                else:  # fetch_mode == "none"
                    query_result = {
                        "query_index": i,
                        "status": str(result),
                        "operation_completed": True,
                    }

                formatted_results.append(query_result)

            except Exception as e:
                logger.error(f"Error formatting result for query {i}: {e}")
                raise Exception(f"Failed to format result for query {i}: {e}") from e

        transaction_result = {
            "transaction_results": formatted_results,
            "query_count": len(prepared_queries),
            "execution_time_ms": round(execution_time * 1000, 2),
            "metadata": {
                "transaction_completed": True,
                "all_queries_successful": True,
                "rollback_occurred": False,
            },
        }

        logger.info(f"Transaction completed successfully in {execution_time:.3f}s with {len(prepared_queries)} queries")
        return format_success_response(
            data=transaction_result, message="Transaction executed successfully"
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Transaction error: {e}")
        return format_error_response(e.error_code, str(e), getattr(e, "details", {}))

    except Exception as e:
        logger.error(f"Unexpected transaction error: {e}")
        mcp_error = handle_postgres_error(e)
        if hasattr(mcp_error, "details"):
            mcp_error.details["transaction_failed"] = True
            mcp_error.details["rollback_occurred"] = True
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


# Tool schema definitions for MCP registration
EXECUTE_QUERY_SCHEMA = {
    "name": "execute_query",
    "description": "Execute a parameterized SQL query safely with parameter binding to prevent SQL injection",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query with parameter placeholders ($1, $2, etc.)",
            },
            "parameters": {
                "type": "array",
                "description": "List of parameters to bind to the query placeholders",
                "items": {"type": ["string", "number", "boolean", "null"]},
                "default": [],
            },
            "fetch_mode": {
                "type": "string",
                "description": "How to fetch results: 'all' (all rows), 'one' (single row), 'none' (status only), 'val' (single value)",
                "enum": ["all", "one", "none", "val"],
                "default": "all",
            },
        },
        "required": ["query"],
    },
}

EXECUTE_RAW_QUERY_SCHEMA = {
    "name": "execute_raw_query",
    "description": "Execute a raw SQL query without parameter binding (WARNING: potential SQL injection risk)",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Raw SQL query to execute (ensure input is trusted)",
            },
            "fetch_mode": {
                "type": "string",
                "description": "How to fetch results: 'all' (all rows), 'one' (single row), 'none' (status only), 'val' (single value)",
                "enum": ["all", "one", "none", "val"],
                "default": "all",
            },
        },
        "required": ["query"],
    },
}

EXECUTE_TRANSACTION_SCHEMA = {
    "name": "execute_transaction",
    "description": "Execute multiple queries in a single transaction with automatic rollback on failure",
    "inputSchema": {
        "type": "object",
        "properties": {
            "queries": {
                "type": "array",
                "description": "List of query objects to execute in transaction",
                "items": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "SQL query with parameter placeholders",
                        },
                        "parameters": {
                            "type": "array",
                            "description": "Parameters for the query",
                            "items": {"type": ["string", "number", "boolean", "null"]},
                            "default": [],
                        },
                        "fetch_mode": {
                            "type": "string",
                            "description": "How to fetch results for this query",
                            "enum": ["all", "one", "none", "val"],
                            "default": "all",
                        },
                    },
                    "required": ["query"],
                },
                "minItems": 1,
            }
        },
        "required": ["queries"],
    },
}

# Export tool functions and schemas
__all__ = [
    "execute_query",
    "execute_raw_query",
    "execute_transaction",
    "EXECUTE_QUERY_SCHEMA",
    "EXECUTE_RAW_QUERY_SCHEMA",
    "EXECUTE_TRANSACTION_SCHEMA",
]
