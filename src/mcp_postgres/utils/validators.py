"""Input validation utilities for MCP Postgres server.

This module provides validation functions for sanitizing and validating
input parameters to prevent SQL injection and ensure data integrity.
"""

import re
from decimal import Decimal
from typing import Any


def validate_table_name(table_name: str) -> bool:
    """Validate PostgreSQL table name format.

    Args:
        table_name: Table name to validate

    Returns:
        True if valid table name format

    Raises:
        ValueError: If table name is invalid
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError("Table name must be a non-empty string")

    # PostgreSQL identifier rules: start with letter/underscore, contain letters/digits/underscores
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"Invalid table name format: {table_name}")

    # Check length limit (PostgreSQL max identifier length is 63)
    if len(table_name) > 63:
        raise ValueError(f"Table name too long (max 63 characters): {table_name}")

    return True


def validate_column_name(column_name: str) -> bool:
    """Validate PostgreSQL column name format.

    Args:
        column_name: Column name to validate

    Returns:
        True if valid column name format

    Raises:
        ValueError: If column name is invalid
    """
    if not column_name or not isinstance(column_name, str):
        raise ValueError("Column name must be a non-empty string")

    # Same rules as table names
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name):
        raise ValueError(f"Invalid column name format: {column_name}")

    if len(column_name) > 63:
        raise ValueError(f"Column name too long (max 63 characters): {column_name}")

    return True


def validate_query_parameters(parameters: list[Any]) -> list[Any]:
    """Validate and sanitize query parameters.

    Args:
        parameters: List of query parameters

    Returns:
        Validated and sanitized parameters

    Raises:
        ValueError: If parameters contain invalid types
    """
    if not isinstance(parameters, list):
        raise ValueError("Parameters must be a list")

    validated_params: list[Any] = []

    for i, param in enumerate(parameters):
        # Allow None, basic types, and Decimal
        if param is None or isinstance(
            param, str | int | float | bool | Decimal | bytes
        ):
            validated_params.append(param)
        else:
            raise ValueError(f"Invalid parameter type at index {i}: {type(param)}")

    return validated_params


def validate_limit_offset(
    limit: int | None = None, offset: int | None = None
) -> tuple[int | None, int | None]:
    """Validate LIMIT and OFFSET values.

    Args:
        limit: Maximum number of rows to return
        offset: Number of rows to skip

    Returns:
        Tuple of validated (limit, offset)

    Raises:
        ValueError: If values are invalid
    """
    if limit is not None:
        if not isinstance(limit, int) or limit < 0:
            raise ValueError("LIMIT must be a non-negative integer")
        if limit > 10000:  # Reasonable upper bound
            raise ValueError("LIMIT too large (max 10000)")

    if offset is not None:
        if not isinstance(offset, int) or offset < 0:
            raise ValueError("OFFSET must be a non-negative integer")

    return limit, offset


def validate_sql_query_pattern(query: str) -> bool:
    """Validate SQL query for dangerous patterns.

    Args:
        query: SQL query string to validate

    Returns:
        True if query passes basic safety checks

    Raises:
        ValueError: If query contains dangerous patterns
    """
    if not query or not isinstance(query, str):
        raise ValueError("Query must be a non-empty string")

    query_upper = query.upper().strip()

    # Check for dangerous patterns
    dangerous_patterns = [
        r"\bDROP\s+TABLE\b",
        r"\bDROP\s+DATABASE\b",
        r"\bTRUNCATE\b",
        r"\bALTER\s+TABLE\b",
        r"\bCREATE\s+TABLE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r";\s*DROP\b",
        r";\s*DELETE\b",
        r";\s*UPDATE\b",
        r"--",  # SQL comments
        r"/\*",  # Block comments
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, query_upper):
            raise ValueError(f"Query contains potentially dangerous pattern: {pattern}")

    return True


def validate_data_type(value: Any, expected_type: str) -> bool:
    """Validate value matches expected PostgreSQL data type.

    Args:
        value: Value to validate
        expected_type: Expected PostgreSQL data type

    Returns:
        True if value matches expected type

    Raises:
        ValueError: If value doesn't match expected type
    """
    if value is None:
        return True  # NULL is valid for any type

    type_validators = {
        "integer": lambda v: isinstance(v, int),
        "bigint": lambda v: isinstance(v, int),
        "smallint": lambda v: isinstance(v, int) and -32768 <= v <= 32767,
        "numeric": lambda v: isinstance(v, int | float | Decimal),
        "decimal": lambda v: isinstance(v, int | float | Decimal),
        "real": lambda v: isinstance(v, int | float),
        "double precision": lambda v: isinstance(v, int | float),
        "text": lambda v: isinstance(v, str),
        "varchar": lambda v: isinstance(v, str),
        "char": lambda v: isinstance(v, str),
        "boolean": lambda v: isinstance(v, bool),
        "date": lambda v: isinstance(
            v, str
        ),  # Simplified - would need proper date parsing
        "timestamp": lambda v: isinstance(v, str),  # Simplified
        "json": lambda v: isinstance(v, str | dict | list),
        "jsonb": lambda v: isinstance(v, str | dict | list),
    }

    validator = type_validators.get(expected_type.lower())
    if not validator:
        # Unknown type, allow it
        return True

    if not validator(value):
        raise ValueError(f"Value {value} is not valid for type {expected_type}")

    return True


def validate_connection_params(params: dict[str, Any]) -> dict[str, Any]:
    """Validate database connection parameters.

    Args:
        params: Connection parameters dictionary

    Returns:
        Validated connection parameters

    Raises:
        ValueError: If required parameters are missing or invalid
    """
    required_params = ["host", "database", "user"]

    for param in required_params:
        if param not in params or not params[param]:
            raise ValueError(f"Missing required connection parameter: {param}")

    # Validate host format (basic check)
    host = params["host"]
    if not isinstance(host, str) or not re.match(r"^[a-zA-Z0-9.-]+$", host):
        raise ValueError(f"Invalid host format: {host}")

    # Validate port if provided
    if "port" in params:
        port = params["port"]
        if not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError(f"Invalid port number: {port}")

    # Validate database name
    database = params["database"]
    if not isinstance(database, str) or not re.match(r"^[a-zA-Z0-9_-]+$", database):
        raise ValueError(f"Invalid database name: {database}")

    return params


def sanitize_string_input(input_str: str, max_length: int = 1000) -> str:
    """Sanitize string input by removing dangerous characters.

    Args:
        input_str: String to sanitize
        max_length: Maximum allowed length

    Returns:
        Sanitized string

    Raises:
        ValueError: If input is invalid
    """
    if not isinstance(input_str, str):
        raise ValueError("Input must be a string")

    if len(input_str) > max_length:
        raise ValueError(f"Input too long (max {max_length} characters)")

    # Remove null bytes and control characters except newlines and tabs
    sanitized = "".join(char for char in input_str if ord(char) >= 32 or char in "\n\t")

    return sanitized
