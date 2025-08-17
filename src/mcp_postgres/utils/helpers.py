"""Helper utility functions for MCP Postgres server.

This module provides common utility functions used across the MCP Postgres
server for data processing, string manipulation, and general operations.
"""

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlparse


def parse_connection_string(connection_string: str) -> dict[str, Any]:
    """Parse PostgreSQL connection string into components.

    Args:
        connection_string: PostgreSQL connection string (URL format)

    Returns:
        Dictionary with connection parameters

    Raises:
        ValueError: If connection string format is invalid
    """
    if not connection_string.startswith(('postgresql://', 'postgres://')):
        raise ValueError("Connection string must start with postgresql:// or postgres://")

    try:
        parsed = urlparse(connection_string)

        params = {
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 5432,
            'database': parsed.path.lstrip('/') if parsed.path else '',
            'user': parsed.username or '',
            'password': parsed.password or ''
        }

        # Parse query parameters
        if parsed.query:
            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                if values:  # Take first value if multiple
                    params[key] = values[0]

        return params

    except Exception as e:
        raise ValueError(f"Invalid connection string format: {e}") from e


def build_connection_string(host: str,
                          port: int,
                          database: str,
                          user: str,
                          password: str,
                          **kwargs: Any) -> str:
    """Build PostgreSQL connection string from components.

    Args:
        host: Database host
        port: Database port
        database: Database name
        user: Username
        password: Password
        **kwargs: Additional connection parameters

    Returns:
        PostgreSQL connection string
    """
    base_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    if kwargs:
        query_params = []
        for key, value in kwargs.items():
            query_params.append(f"{key}={value}")

        if query_params:
            base_url += "?" + "&".join(query_params)

    return base_url


def extract_table_names(query: str) -> list[str]:
    """Extract table names from SQL query.

    Args:
        query: SQL query string

    Returns:
        List of table names found in the query
    """
    # Simple regex-based extraction (not perfect but covers common cases)
    table_patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        r'\bDELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    ]

    tables = set()
    query_upper = query.upper()

    for pattern in table_patterns:
        matches = re.findall(pattern, query_upper)
        tables.update(matches)

    return list(tables)


def generate_query_hash(query: str, parameters: list[Any] | None = None) -> str:
    """Generate hash for query caching purposes.

    Args:
        query: SQL query string
        parameters: Query parameters

    Returns:
        SHA-256 hash of query and parameters
    """
    # Normalize query (remove extra whitespace)
    normalized_query = ' '.join(query.split())

    # Create hash input
    hash_input = normalized_query
    if parameters:
        # Convert parameters to string representation
        param_str = json.dumps(parameters, sort_keys=True, default=str)
        hash_input += param_str

    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Maximum size of each chunk

    Returns:
        List of chunks
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")

    chunks = []
    for i in range(0, len(items), chunk_size):
        chunks.append(items[i:i + chunk_size])

    return chunks


def flatten_dict(data: dict[str, Any],
                separator: str = '.',
                prefix: str = '') -> dict[str, Any]:
    """Flatten nested dictionary structure.

    Args:
        data: Dictionary to flatten
        separator: Separator for nested keys
        prefix: Prefix for keys

    Returns:
        Flattened dictionary
    """
    flattened = {}

    for key, value in data.items():
        new_key = f"{prefix}{separator}{key}" if prefix else key

        if isinstance(value, dict):
            flattened.update(flatten_dict(value, separator, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_dict(item, separator, f"{new_key}[{i}]"))
                else:
                    flattened[f"{new_key}[{i}]"] = item
        else:
            flattened[new_key] = value

    return flattened


def deep_merge_dicts(dict1: dict[str, Any], dict2: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries.

    Args:
        dict1: First dictionary
        dict2: Second dictionary (takes precedence)

    Returns:
        Merged dictionary
    """
    result = dict1.copy()

    for key, value in dict2.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value

    return result


def safe_cast(value: Any, target_type: type, default: Any = None) -> Any:
    """Safely cast value to target type with fallback.

    Args:
        value: Value to cast
        target_type: Target type to cast to
        default: Default value if casting fails

    Returns:
        Cast value or default
    """
    try:
        if target_type is bool and isinstance(value, str):
            # Special handling for boolean strings
            return value.lower() in ('true', '1', 'yes', 'on')
        return target_type(value)
    except (ValueError, TypeError):
        return default


def get_current_timestamp() -> str:
    """Get current timestamp in ISO format with timezone.

    Returns:
        ISO formatted timestamp string
    """
    return datetime.now(UTC).isoformat()


def calculate_similarity(str1: str, str2: str) -> float:
    """Calculate similarity between two strings using Levenshtein distance.

    Args:
        str1: First string
        str2: Second string

    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not str1 and not str2:
        return 1.0
    if not str1 or not str2:
        return 0.0

    # Simple Levenshtein distance implementation
    len1, len2 = len(str1), len(str2)
    matrix = [[0] * (len2 + 1) for _ in range(len1 + 1)]

    for i in range(len1 + 1):
        matrix[i][0] = i
    for j in range(len2 + 1):
        matrix[0][j] = j

    for i in range(1, len1 + 1):
        for j in range(1, len2 + 1):
            cost = 0 if str1[i-1] == str2[j-1] else 1
            matrix[i][j] = min(
                matrix[i-1][j] + 1,      # deletion
                matrix[i][j-1] + 1,      # insertion
                matrix[i-1][j-1] + cost  # substitution
            )

    max_len = max(len1, len2)
    distance = matrix[len1][len2]

    return 1.0 - (distance / max_len)


def extract_sql_operation(query: str) -> str:
    """Extract the main SQL operation from a query.

    Args:
        query: SQL query string

    Returns:
        Main operation (SELECT, INSERT, UPDATE, DELETE, etc.)
    """
    query_stripped = query.strip().upper()

    # Common SQL operations
    operations = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
        'ALTER', 'TRUNCATE', 'GRANT', 'REVOKE', 'WITH'
    ]

    for operation in operations:
        if query_stripped.startswith(operation):
            return operation

    return 'UNKNOWN'


def is_read_only_query(query: str) -> bool:
    """Check if SQL query is read-only.

    Args:
        query: SQL query string

    Returns:
        True if query is read-only
    """
    operation = extract_sql_operation(query)
    read_only_operations = {'SELECT', 'WITH', 'EXPLAIN', 'SHOW', 'DESCRIBE'}

    return operation in read_only_operations


def sanitize_identifier(identifier: str) -> str:
    """Sanitize database identifier (table/column name).

    Args:
        identifier: Database identifier to sanitize

    Returns:
        Sanitized identifier
    """
    # Remove non-alphanumeric characters except underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', identifier)

    # Ensure it starts with letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
        sanitized = '_' + sanitized

    # Limit length
    return sanitized[:63]  # PostgreSQL identifier limit


def format_sql_query(query: str) -> str:
    """Basic SQL query formatting for readability.

    Args:
        query: SQL query to format

    Returns:
        Formatted SQL query
    """
    # Basic formatting - add line breaks after major keywords
    keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']

    formatted = query
    for keyword in keywords:
        formatted = re.sub(
            f'\\b{keyword}\\b',
            f'\n{keyword}',
            formatted,
            flags=re.IGNORECASE
        )

    # Clean up extra whitespace
    lines = [line.strip() for line in formatted.split('\n') if line.strip()]
    return '\n'.join(lines)


def validate_json_string(json_str: str) -> tuple[bool, dict[str, Any] | None]:
    """Validate and parse JSON string.

    Args:
        json_str: JSON string to validate

    Returns:
        Tuple of (is_valid, parsed_data)
    """
    try:
        parsed = json.loads(json_str)
        return True, parsed
    except json.JSONDecodeError:
        return False, None


def create_pagination_info(total_count: int,
                          page_size: int,
                          current_page: int) -> dict[str, Any]:
    """Create pagination information dictionary.

    Args:
        total_count: Total number of items
        page_size: Items per page
        current_page: Current page number (1-based)

    Returns:
        Pagination information dictionary
    """
    total_pages = (total_count + page_size - 1) // page_size

    return {
        "total_count": total_count,
        "page_size": page_size,
        "current_page": current_page,
        "total_pages": total_pages,
        "has_next": current_page < total_pages,
        "has_previous": current_page > 1,
        "start_index": (current_page - 1) * page_size + 1,
        "end_index": min(current_page * page_size, total_count)
    }


def mask_sensitive_data(data: str, mask_char: str = '*', visible_chars: int = 4) -> str:
    """Mask sensitive data for logging/display.

    Args:
        data: Sensitive data to mask
        mask_char: Character to use for masking
        visible_chars: Number of characters to leave visible at the end

    Returns:
        Masked string
    """
    if len(data) <= visible_chars:
        return mask_char * len(data)

    masked_length = len(data) - visible_chars
    return mask_char * masked_length + data[-visible_chars:]
