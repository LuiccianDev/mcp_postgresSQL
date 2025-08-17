"""Security validation for MCP Postgres server."""

import logging
import re
from enum import Enum
from typing import Any


logger = logging.getLogger(__name__)


class QueryType(Enum):
    """Enumeration of SQL query types."""

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE = "CREATE"
    DROP = "DROP"
    ALTER = "ALTER"
    TRUNCATE = "TRUNCATE"
    GRANT = "GRANT"
    REVOKE = "REVOKE"
    UNKNOWN = "UNKNOWN"


class SecurityError(Exception):
    """Base exception for security-related errors."""

    pass


class SQLInjectionError(SecurityError):
    """Exception raised when potential SQL injection is detected."""

    pass


class AccessDeniedError(SecurityError):
    """Exception raised when access to a resource is denied."""

    pass


class SecurityValidator:
    """Security validation and sanitization for database operations."""

    # Dangerous SQL patterns that should be blocked
    DANGEROUS_PATTERNS = [
        r";\s*(DROP|DELETE|TRUNCATE|ALTER)\s+",  # Command injection
        r"UNION\s+SELECT",  # Union-based injection
        r"--\s*",  # SQL comments
        r"/\*.*\*/",  # Multi-line comments
        r"xp_cmdshell",  # Command execution
        r"sp_executesql",  # Dynamic SQL execution
        r"EXEC\s*\(",  # Execute statements
        r"EXECUTE\s*\(",  # Execute statements
        r"INFORMATION_SCHEMA",  # Schema enumeration
        r"pg_catalog",  # PostgreSQL system catalog
        r"pg_user",  # User enumeration
        r"pg_shadow",  # Password hashes
    ]

    # Allowed table prefixes (whitelist approach)
    ALLOWED_TABLE_PREFIXES = {
        "user_",
        "app_",
        "data_",
        "temp_",
        "staging_",
        "public.",
    }

    # System tables that should be restricted
    SYSTEM_TABLES = {
        "pg_user",
        "pg_shadow",
        "pg_group",
        "pg_database",
        "pg_tables",
        "pg_indexes",
        "pg_views",
        "pg_roles",
        "information_schema",
        "pg_catalog",
    }

    def __init__(self, allowed_schemas: set[str] | None = None):
        """Initialize security validator.

        Args:
            allowed_schemas: Set of allowed schema names. Defaults to {'public'}.
        """
        self.allowed_schemas = allowed_schemas or {"public"}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Compile regex patterns for performance."""
        self.compiled_patterns = [
            re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            for pattern in self.DANGEROUS_PATTERNS
        ]

    def validate_query_permissions(self, query: str) -> tuple[bool, str | None]:
        """Validate if a query is safe to execute.

        Args:
            query: SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)

        Raises:
            SQLInjectionError: If potential SQL injection is detected
        """
        if not query or not query.strip():
            return False, "Empty query not allowed"

        query_clean = query.strip()

        # Check for dangerous patterns
        for pattern in self.compiled_patterns:
            if pattern.search(query_clean):
                error_msg = (
                    f"Potentially dangerous SQL pattern detected: {pattern.pattern}"
                )
                logger.warning(f"SQL injection attempt blocked: {error_msg}")
                raise SQLInjectionError(error_msg)

        # Validate query type
        query_type = self._get_query_type(query_clean)
        if query_type == QueryType.UNKNOWN:
            return False, "Unknown or unsupported query type"

        # Additional validation based on query type
        if query_type in {QueryType.DROP, QueryType.TRUNCATE}:
            return False, f"{query_type.value} operations are not allowed"

        # Validate table access
        tables = self._extract_table_names(query_clean)
        for table in tables:
            if not self.check_table_access(table):
                return False, f"Access denied to table: {table}"

        return True, None

    def sanitize_parameters(self, params: list[Any]) -> list[Any]:
        """Sanitize query parameters to prevent injection.

        Args:
            params: List of query parameters

        Returns:
            List of sanitized parameters
        """
        if not params:
            return []

        sanitized: list[Any] = []
        for param in params:
            if isinstance(param, str):
                # Remove potentially dangerous characters
                sanitized_param = self._sanitize_string_parameter(param)
                sanitized.append(sanitized_param)
            elif isinstance(param, int | float | bool):
                sanitized.append(param)
            elif param is None:
                sanitized.append(None)
            else:
                # Convert other types to string and sanitize
                sanitized.append(self._sanitize_string_parameter(str(param)))

        return sanitized

    def check_table_access(self, table_name: str) -> bool:
        """Check if access to a table is allowed.

        Args:
            table_name: Name of the table to check

        Returns:
            True if access is allowed, False otherwise
        """
        if not table_name:
            return False

        table_lower = table_name.lower().strip()

        # Block access to system tables
        for system_table in self.SYSTEM_TABLES:
            if system_table in table_lower:
                logger.warning(f"Access denied to system table: {table_name}")
                return False

        # Check schema access
        if "." in table_name:
            schema, _ = table_name.split(".", 1)
            if schema.lower() not in {s.lower() for s in self.allowed_schemas}:
                logger.warning(f"Access denied to schema: {schema}")
                return False

        # For now, allow access to non-system tables
        # In production, this could be more restrictive
        return True

    def validate_column_names(self, columns: list[str]) -> tuple[bool, str | None]:
        """Validate column names for safety.

        Args:
            columns: List of column names to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not columns:
            return True, None

        for column in columns:
            if not self._is_valid_identifier(column):
                return False, f"Invalid column name: {column}"

        return True, None

    def _get_query_type(self, query: str) -> QueryType:
        """Determine the type of SQL query.

        Args:
            query: SQL query to analyze

        Returns:
            QueryType enum value
        """
        query_upper = query.upper().strip()

        for query_type in QueryType:
            if query_type != QueryType.UNKNOWN and query_upper.startswith(
                query_type.value
            ):
                return query_type

        return QueryType.UNKNOWN

    def _extract_table_names(self, query: str) -> set[str]:
        """Extract table names from SQL query.

        Args:
            query: SQL query to analyze

        Returns:
            Set of table names found in the query
        """
        tables = set()

        # Simple regex patterns to find table names
        # This is a basic implementation - in production, you'd want a proper SQL parser
        patterns = [
            r"FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
            r"JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
            r"UPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
            r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
            r"DELETE\s+FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?)",
        ]

        for pattern in patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1).strip()
                if table_name:
                    tables.add(table_name)

        return tables

    def _sanitize_string_parameter(self, param: str) -> str:
        """Sanitize a string parameter.

        Args:
            param: String parameter to sanitize

        Returns:
            Sanitized string parameter
        """
        if not param:
            return param

        # Remove null bytes
        sanitized = param.replace("\x00", "")

        # Remove or escape potentially dangerous characters
        # Note: In practice, parameter binding handles most of this
        dangerous_chars = ["--", "/*", "*/", ";"]
        for char in dangerous_chars:
            sanitized = sanitized.replace(char, "")

        return sanitized

    def _is_valid_identifier(self, identifier: str) -> bool:
        """Check if an identifier (table/column name) is valid.

        Args:
            identifier: Identifier to validate

        Returns:
            True if valid, False otherwise
        """
        if not identifier:
            return False

        # Basic validation for SQL identifiers
        # Must start with letter or underscore, followed by letters, digits, or underscores
        pattern = r"^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?$"
        return bool(re.match(pattern, identifier))


# Global security validator instance
_security_validator: SecurityValidator | None = None


def get_security_validator() -> SecurityValidator:
    """Get the global security validator instance."""
    global _security_validator
    if _security_validator is None:
        _security_validator = SecurityValidator()
    return _security_validator


def validate_query_permissions(query: str) -> tuple[bool, str | None]:
    """Validate if a query is safe to execute.

    Args:
        query: SQL query to validate

    Returns:
        Tuple of (is_valid, error_message)
    """
    return get_security_validator().validate_query_permissions(query)


def sanitize_parameters(params: list[Any]) -> list[Any]:
    """Sanitize query parameters to prevent injection.

    Args:
        params: List of query parameters

    Returns:
        List of sanitized parameters
    """
    return get_security_validator().sanitize_parameters(params)


def check_table_access(table_name: str) -> bool:
    """Check if access to a table is allowed.

    Args:
        table_name: Name of the table to check

    Returns:
        True if access is allowed, False otherwise
    """
    return get_security_validator().check_table_access(table_name)
