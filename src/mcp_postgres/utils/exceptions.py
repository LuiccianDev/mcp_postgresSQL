"""Custom exception hierarchy for MCP Postgres server.

This module defines a comprehensive exception hierarchy for handling
various error conditions in the MCP Postgres server with structured
error responses and proper error categorization.
"""

from typing import Any


class MCPPostgresError(Exception):
    """Base exception class for all MCP Postgres server errors.

    Attributes:
        message: Human-readable error message
        error_code: Standardized error code for programmatic handling
        details: Additional error context and details
    """

    def __init__(self,
                 message: str,
                 error_code: str = "GENERAL_ERROR",
                 details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to dictionary for MCP error responses."""
        return {
            "code": self.error_code,
            "message": self.message,
            "details": self.details
        }


class ConnectionError(MCPPostgresError):
    """Raised when database connection issues occur."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            error_code="CONNECTION_ERROR",
            details=details
        )


class ConnectionPoolError(ConnectionError):
    """Raised when connection pool management fails."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message, details)
        self.error_code = "CONNECTION_POOL_ERROR"


class ConnectionTimeoutError(ConnectionError):
    """Raised when database connection times out."""

    def __init__(self, message: str, timeout_seconds: float | None = None):
        details: dict[str, Any] = {}
        if timeout_seconds:
            details["timeout_seconds"] = timeout_seconds

        super().__init__(message, details)
        self.error_code = "CONNECTION_TIMEOUT_ERROR"


class QueryError(MCPPostgresError):
    """Base class for query execution errors."""

    def __init__(self,
                 message: str,
                 query: str | None = None,
                 parameters: list | None = None,
                 details: dict[str, Any] | None = None):
        query_details = details or {}
        if query:
            query_details["query"] = query
        if parameters:
            query_details["parameters"] = parameters

        super().__init__(
            message=message,
            error_code="QUERY_ERROR",
            details=query_details
        )


class QuerySyntaxError(QueryError):
    """Raised when SQL query has syntax errors."""

    def __init__(self,
                 message: str,
                 query: str | None = None,
                 line: int | None = None,
                 position: int | None = None):
        details: dict[str, Any] = {}
        if line:
            details["line"] = line
        if position:
            details["position"] = position

        super().__init__(message, query, None, details)
        self.error_code = "QUERY_SYNTAX_ERROR"


class QueryExecutionError(QueryError):
    """Raised when query execution fails."""

    def __init__(self,
                 message: str,
                 query: str | None = None,
                 parameters: list | None = None,
                 postgres_error_code: str | None = None):
        details: dict[str, Any] = {}
        if postgres_error_code:
            details["postgres_error_code"] = postgres_error_code

        super().__init__(message, query, parameters, details)
        self.error_code = "QUERY_EXECUTION_ERROR"


class QueryTimeoutError(QueryError):
    """Raised when query execution times out."""

    def __init__(self,
                 message: str,
                 query: str | None = None,
                 timeout_seconds: float | None = None):
        details: dict[str, Any] = {}
        if timeout_seconds:
            details["timeout_seconds"] = timeout_seconds

        super().__init__(message, query, None, details)
        self.error_code = "QUERY_TIMEOUT_ERROR"


class ValidationError(MCPPostgresError):
    """Raised when input validation fails."""

    def __init__(self,
                 message: str,
                 field_name: str | None = None,
                 field_value: Any | None = None,
                 validation_rule: str | None = None):
        details: dict[str, Any] = {}
        if field_name:
            details["field_name"] = field_name
        if field_value is not None:
            details["field_value"] = str(field_value)
        if validation_rule:
            details["validation_rule"] = validation_rule

        super().__init__(
            message=message,
            error_code="VALIDATION_ERROR",
            details=details
        )


class ParameterValidationError(ValidationError):
    """Raised when query parameters fail validation."""

    def __init__(self,
                 message: str,
                 parameter_index: int | None = None,
                 parameter_value: Any | None = None):
        details: dict[str, Any] = {}
        if parameter_index is not None:
            details["parameter_index"] = parameter_index
        if parameter_value is not None:
            details["parameter_value"] = str(parameter_value)

        super().__init__(message, "PARAMETER_VALIDATION_ERROR", details)


class SecurityError(MCPPostgresError):
    """Raised when security validation fails."""

    def __init__(self,
                 message: str,
                 security_rule: str | None = None,
                 attempted_operation: str | None = None):
        details: dict[str, Any] = {}
        if security_rule:
            details["security_rule"] = security_rule
        if attempted_operation:
            details["attempted_operation"] = attempted_operation

        super().__init__(
            message=message,
            error_code="SECURITY_ERROR",
            details=details
        )


class PermissionError(SecurityError):
    """Raised when operation lacks required permissions."""

    def __init__(self,
                 message: str,
                 required_permission: str | None = None,
                 resource: str | None = None):
        details: dict[str, Any] = {}
        if required_permission:
            details["required_permission"] = required_permission
        if resource:
            details["resource"] = resource

        super().__init__(message, None, None)
        self.error_code = "PERMISSION_ERROR"
        self.details.update(details)


class SQLInjectionError(SecurityError):
    """Raised when potential SQL injection is detected."""

    def __init__(self,
                 message: str,
                 dangerous_pattern: str | None = None,
                 query_fragment: str | None = None):
        details: dict[str, Any] = {}
        if dangerous_pattern:
            details["dangerous_pattern"] = dangerous_pattern
        if query_fragment:
            details["query_fragment"] = query_fragment

        super().__init__(message, None, None)
        self.error_code = "SQL_INJECTION_ERROR"
        self.details.update(details)


class ConfigurationError(MCPPostgresError):
    """Raised when configuration is invalid or missing."""

    def __init__(self,
                 message: str,
                 config_key: str | None = None,
                 config_value: str | None = None):
        details: dict[str, Any] = {}
        if config_key:
            details["config_key"] = config_key
        if config_value:
            details["config_value"] = config_value

        super().__init__(
            message=message,
            error_code="CONFIGURATION_ERROR",
            details=details
        )


class DatabaseError(MCPPostgresError):
    """Raised when database-level errors occur."""

    def __init__(self,
                 message: str,
                 database_name: str | None = None,
                 postgres_error_code: str | None = None):
        details: dict[str, Any] = {}
        if database_name:
            details["database_name"] = database_name
        if postgres_error_code:
            details["postgres_error_code"] = postgres_error_code

        super().__init__(
            message=message,
            error_code="DATABASE_ERROR",
            details=details
        )


class TableNotFoundError(DatabaseError):
    """Raised when referenced table doesn't exist."""

    def __init__(self,
                 table_name: str,
                 schema_name: str | None = None):
        message = f"Table '{table_name}' not found"
        if schema_name:
            message = f"Table '{schema_name}.{table_name}' not found"

        details: dict[str, Any] = {"table_name": table_name}
        if schema_name:
            details["schema_name"] = schema_name

        super().__init__(message, None, None)
        self.error_code = "TABLE_NOT_FOUND_ERROR"
        self.details.update(details)


class ColumnNotFoundError(DatabaseError):
    """Raised when referenced column doesn't exist."""

    def __init__(self,
                 column_name: str,
                 table_name: str | None = None):
        message = f"Column '{column_name}' not found"
        if table_name:
            message = f"Column '{column_name}' not found in table '{table_name}'"

        details: dict[str, Any] = {"column_name": column_name}
        if table_name:
            details["table_name"] = table_name

        super().__init__(message, None, None)
        self.error_code = "COLUMN_NOT_FOUND_ERROR"
        self.details.update(details)


class TransactionError(MCPPostgresError):
    """Raised when transaction operations fail."""

    def __init__(self,
                 message: str,
                 transaction_state: str | None = None,
                 rollback_attempted: bool = False):
        details: dict[str, Any] = {}
        if transaction_state:
            details["transaction_state"] = transaction_state
        details["rollback_attempted"] = rollback_attempted

        super().__init__(
            message=message,
            error_code="TRANSACTION_ERROR",
            details=details
        )


class DataIntegrityError(MCPPostgresError):
    """Raised when data integrity constraints are violated."""

    def __init__(self,
                 message: str,
                 constraint_name: str | None = None,
                 constraint_type: str | None = None,
                 table_name: str | None = None):
        details: dict[str, Any] = {}
        if constraint_name:
            details["constraint_name"] = constraint_name
        if constraint_type:
            details["constraint_type"] = constraint_type
        if table_name:
            details["table_name"] = table_name

        super().__init__(
            message=message,
            error_code="DATA_INTEGRITY_ERROR",
            details=details
        )


class ToolError(MCPPostgresError):
    """Raised when MCP tool execution fails."""

    def __init__(self,
                 message: str,
                 tool_name: str | None = None,
                 tool_parameters: dict[str, Any] | None = None):
        details: dict[str, Any] = {}
        if tool_name:
            details["tool_name"] = tool_name
        if tool_parameters:
            details["tool_parameters"] = tool_parameters

        super().__init__(
            message=message,
            error_code="TOOL_ERROR",
            details=details
        )


class ToolNotFoundError(ToolError):
    """Raised when requested MCP tool doesn't exist."""

    def __init__(self, tool_name: str):
        super().__init__(
            message=f"Tool '{tool_name}' not found",
            tool_name=tool_name
        )
        self.error_code = "TOOL_NOT_FOUND_ERROR"


class ToolParameterError(ToolError):
    """Raised when tool parameters are invalid."""

    def __init__(self,
                 message: str,
                 tool_name: str,
                 parameter_name: str | None = None,
                 parameter_value: Any | None = None):
        details: dict[str, Any] = {"tool_name": tool_name}
        if parameter_name:
            details["parameter_name"] = parameter_name
        if parameter_value is not None:
            details["parameter_value"] = str(parameter_value)

        super().__init__(message, tool_name, None)
        self.error_code = "TOOL_PARAMETER_ERROR"
        self.details.update(details)


def handle_postgres_error(pg_error: Exception,
                         query: str | None = None,
                         parameters: list | None = None) -> MCPPostgresError:
    """Convert PostgreSQL errors to appropriate MCP Postgres exceptions.

    Args:
        pg_error: Original PostgreSQL error
        query: SQL query that caused the error
        parameters: Query parameters

    Returns:
        Appropriate MCPPostgresError subclass
    """
    error_message = str(pg_error)

    # Map common PostgreSQL error codes to our exceptions
    if hasattr(pg_error, 'sqlstate'):
        sqlstate = pg_error.sqlstate

        # Connection errors (08xxx)
        if sqlstate.startswith('08'):
            return ConnectionError(error_message, {"postgres_sqlstate": sqlstate})

        # Syntax errors (42601)
        elif sqlstate == '42601':
            return QuerySyntaxError(error_message, query)

        # Undefined table (42P01)
        elif sqlstate == '42P01':
            return TableNotFoundError("Table referenced in query", None)

        # Undefined column (42703)
        elif sqlstate == '42703':
            return ColumnNotFoundError("Column referenced in query", None)

        # Integrity constraint violation (23xxx)
        elif sqlstate.startswith('23'):
            return DataIntegrityError(error_message, None, "constraint_violation")

        # Transaction errors (25xxx)
        elif sqlstate.startswith('25'):
            return TransactionError(error_message)

    # Default to generic query execution error
    return QueryExecutionError(error_message, query, parameters)
