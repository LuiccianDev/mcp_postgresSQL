"""Output formatting utilities for MCP Postgres server.

This module provides functions for formatting query results and responses
into consistent, structured formats for MCP tool responses.
"""

from datetime import date, datetime, time
from decimal import Decimal
from typing import Any


def format_query_result(
    rows: list[dict[str, Any]],
    columns: list[str],
    execution_time: float,
    row_count: int | None = None,
) -> dict[str, Any]:
    """Format database query results into standardized response.

    Args:
        rows: Query result rows
        columns: Column names
        execution_time: Query execution time in seconds
        row_count: Number of rows affected (for INSERT/UPDATE/DELETE)

    Returns:
        Formatted query result dictionary
    """
    # Convert any non-serializable values
    formatted_rows = []
    for row in rows:
        formatted_row = {}
        for key, value in row.items():
            formatted_row[key] = serialize_value(value)
        formatted_rows.append(formatted_row)

    result = {
        "rows": formatted_rows,
        "columns": columns,
        "row_count": len(formatted_rows) if row_count is None else row_count,
        "execution_time_ms": round(execution_time * 1000, 2),
        "metadata": {
            "has_results": len(formatted_rows) > 0,
            "column_count": len(columns),
        },
    }

    return result


def format_table_info(
    table_name: str,
    columns: list[dict[str, Any]],
    indexes: list[dict[str, Any]] | None = None,
    constraints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Format table information into structured response.

    Args:
        table_name: Name of the table
        columns: List of column information dictionaries
        indexes: Optional list of index information
        constraints: Optional list of constraint information

    Returns:
        Formatted table information dictionary
    """
    result = {
        "table_name": table_name,
        "columns": [serialize_dict(col) for col in columns],
        "column_count": len(columns),
        "metadata": {
            "has_primary_key": any(col.get("is_primary_key", False) for col in columns),
            "nullable_columns": sum(
                1 for col in columns if col.get("is_nullable", True)
            ),
            "indexed_columns": len(indexes) if indexes else 0,
        },
    }

    if indexes:
        result["indexes"] = [serialize_dict(idx) for idx in indexes]

    if constraints:
        result["constraints"] = [serialize_dict(const) for const in constraints]

    return result


def format_analysis_result(
    analysis_type: str,
    table_name: str,
    column_name: str | None = None,
    results: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Format data analysis results.

    Args:
        analysis_type: Type of analysis performed
        table_name: Name of analyzed table
        column_name: Name of analyzed column (if applicable)
        results: Analysis results dictionary

    Returns:
        Formatted analysis result dictionary
    """
    result = {
        "analysis_type": analysis_type,
        "table_name": table_name,
        "timestamp": datetime.now().isoformat(),
        "results": serialize_dict(results or {}),
    }

    if column_name:
        result["column_name"] = column_name

    return result


def format_error_response(
    error_code: str, message: str, details: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Format error responses in consistent structure.

    Args:
        error_code: Standardized error code
        message: Human-readable error message
        details: Optional additional error details

    Returns:
        Formatted error response dictionary
    """
    error_info: dict[str, Any] = {
        "code": error_code,
        "message": message,
        "timestamp": datetime.now().isoformat(),
    }

    if details:
        error_info["details"] = serialize_dict(details)

    error_response = {"error": error_info}

    return error_response


def format_success_response(
    data: Any, message: str | None = None, metadata: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Format successful operation responses.

    Args:
        data: Response data
        message: Optional success message
        metadata: Optional metadata

    Returns:
        Formatted success response dictionary
    """
    response: dict[str, Any] = {
        "success": True,
        "data": serialize_value(data),
        "timestamp": datetime.now().isoformat(),
    }

    if message:
        response["message"] = message

    if metadata:
        response["metadata"] = serialize_dict(metadata)

    return response


def format_table_list(tables: list[dict[str, Any]]) -> dict[str, Any]:
    """Format list of tables with metadata.

    Args:
        tables: List of table information dictionaries

    Returns:
        Formatted table list response
    """
    formatted_tables = []
    total_size = 0

    for table in tables:
        formatted_table = serialize_dict(table)
        formatted_tables.append(formatted_table)

        # Sum up table sizes if available
        if "size_bytes" in table and table["size_bytes"]:
            total_size += table["size_bytes"]

    return {
        "tables": formatted_tables,
        "table_count": len(formatted_tables),
        "total_size_bytes": total_size,
        "total_size_human": format_bytes(total_size),
        "metadata": {
            "has_size_info": any("size_bytes" in t for t in tables),
            "timestamp": datetime.now().isoformat(),
        },
    }


def format_performance_stats(
    stats: dict[str, Any], query: str | None = None
) -> dict[str, Any]:
    """Format performance statistics.

    Args:
        stats: Performance statistics dictionary
        query: Optional query that was analyzed

    Returns:
        Formatted performance statistics
    """
    result = {
        "performance_stats": serialize_dict(stats),
        "timestamp": datetime.now().isoformat(),
    }

    if query:
        result["analyzed_query"] = query

    # Add human-readable interpretations
    if "execution_time_ms" in stats:
        exec_time = stats["execution_time_ms"]
        if exec_time < 100:
            result["performance_rating"] = "excellent"
        elif exec_time < 1000:
            result["performance_rating"] = "good"
        elif exec_time < 5000:
            result["performance_rating"] = "moderate"
        else:
            result["performance_rating"] = "poor"

    return result


def serialize_value(value: Any) -> Any:
    """Serialize a value to JSON-compatible format.

    Args:
        value: Value to serialize

    Returns:
        JSON-serializable value
    """
    if value is None:
        return None
    elif isinstance(value, str | int | float | bool):
        return value
    elif isinstance(value, Decimal):
        return float(value)
    elif isinstance(value, datetime | date):
        return value.isoformat()
    elif isinstance(value, time):
        return value.isoformat()
    elif isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    elif isinstance(value, list | tuple):
        return [serialize_value(item) for item in value]
    elif isinstance(value, dict):
        return serialize_dict(value)
    elif hasattr(value, "__dict__"):
        return serialize_dict(value.__dict__)
    else:
        return str(value)


def serialize_dict(data: dict[str, Any]) -> dict[str, Any]:
    """Serialize dictionary values to JSON-compatible format.

    Args:
        data: Dictionary to serialize

    Returns:
        Dictionary with serialized values
    """
    """ if not isinstance(data, dict):
        return serialize_value(data) """

    result = {}
    for key, value in data.items():
        result[str(key)] = serialize_value(value)

    return result


def format_bytes(bytes_value: int) -> str:
    """Format byte count into human-readable string.

    Args:
        bytes_value: Number of bytes

    Returns:
        Human-readable byte count (e.g., "1.5 MB")
    """
    if bytes_value == 0:
        return "0 B"

    units = ["B", "KB", "MB", "GB", "TB"]
    unit_index = 0
    size = float(bytes_value)

    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1

    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Human-readable duration (e.g., "1.5s", "2m 30s")
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    elif seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        remaining_seconds = seconds % 60
        return f"{minutes}m {remaining_seconds:.0f}s"
    else:
        hours = int(seconds // 3600)
        remaining_minutes = int((seconds % 3600) // 60)
        return f"{hours}h {remaining_minutes}m"


def truncate_text(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """Truncate text to specified length with suffix.

    Args:
        text: Text to truncate
        max_length: Maximum length before truncation
        suffix: Suffix to add when truncating

    Returns:
        Truncated text with suffix if needed
    """
    """ if not isinstance(text, str):
        text = str(text) """

    if len(text) <= max_length:
        return text

    return text[: max_length - len(suffix)] + suffix
