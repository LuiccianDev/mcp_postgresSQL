"""MCP context management for MCP Postgres server."""

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from ..config.settings import server_config
from ..utils.logging import LogContext, PerformanceMetrics, get_logger


logger = get_logger(__name__)


@dataclass
class ToolExecutionContext:
    """Context information for tool execution."""

    tool_name: str
    execution_id: str = field(default_factory=lambda: str(uuid4()))
    start_time: float = field(default_factory=time.time)
    parameters: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    end_time: float | None = None
    success: bool = False
    error: str | None = None
    result_size: int | None = None

    @property
    def execution_time(self) -> float | None:
        """Calculate execution time in seconds."""
        if self.end_time is not None:
            return self.end_time - self.start_time
        return None


@dataclass
class MCPError:
    """Structured MCP error response."""

    code: str
    message: str
    details: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert error to dictionary format."""
        error_dict: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            error_dict["details"] = self.details
        return error_dict


class MCPContextManager:
    """Manages MCP tool execution context and error handling."""

    def __init__(self) -> None:
        """Initialize the context manager."""
        self._active_contexts: dict[str, ToolExecutionContext] = {}
        self._execution_stats: dict[str, Any] = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "total_execution_time": 0.0,
            "tool_usage_count": {},
        }

    @asynccontextmanager
    async def tool_execution_context(
        self, tool_name: str, parameters: dict[str, Any] | None = None
    ) -> AsyncGenerator[ToolExecutionContext]:
        """Context manager for tool execution with automatic logging and error handling.

        Args:
            tool_name: Name of the tool being executed
            parameters: Tool parameters

        Yields:
            ToolExecutionContext: Context object for the tool execution
        """
        parameters = parameters or {}
        context = ToolExecutionContext(tool_name=tool_name, parameters=parameters)

        # Store active context
        self._active_contexts[context.execution_id] = context

        # Create log context
        log_context = LogContext(tool_name=tool_name, operation="tool_execution")

        # Log tool execution start
        with logger.log_context(log_context):
            logger.info(
                f"Starting tool execution: {tool_name}",
                log_context,
                {
                    "execution_id": context.execution_id,
                    "parameters": parameters
                    if server_config.log_query_parameters
                    else None,
                },
            )

        # Update usage statistics
        self._execution_stats["total_executions"] += 1
        self._execution_stats["tool_usage_count"][tool_name] = (
            self._execution_stats["tool_usage_count"].get(tool_name, 0) + 1
        )

        try:
            yield context

            # Mark as successful
            context.success = True
            context.end_time = time.time()

            # Update success statistics
            self._execution_stats["successful_executions"] += 1
            self._execution_stats["total_execution_time"] += context.execution_time or 0

            # Log successful completion with performance metrics
            if context.execution_time:
                metrics = PerformanceMetrics(
                    execution_time_ms=context.execution_time * 1000,
                    query_count=1,
                    result_size=context.result_size or 0,
                )
                logger.log_performance(
                    f"tool_execution_{tool_name}", metrics, log_context
                )

            logger.info(
                f"Tool execution completed successfully: {tool_name}",
                log_context,
                {
                    "execution_id": context.execution_id,
                    "duration_ms": round((context.execution_time or 0) * 1000, 2),
                },
            )

        except Exception as e:
            # Mark as failed and capture error
            context.success = False
            context.end_time = time.time()
            context.error = str(e)

            # Update failure statistics
            self._execution_stats["failed_executions"] += 1

            # Log error with context
            logger.log_error(
                error=e,
                operation=f"tool_execution_{tool_name}",
                context=log_context,
                additional_data={
                    "execution_id": context.execution_id,
                    "duration_ms": round((context.execution_time or 0) * 1000, 2),
                },
            )

            # Re-raise the exception
            raise

        finally:
            # Remove from active contexts
            self._active_contexts.pop(context.execution_id, None)

    def create_mcp_error(
        self,
        error_code: str,
        message: str,
        details: dict[str, Any] | None = None,
        original_error: Exception | None = None,
    ) -> MCPError:
        """Create a structured MCP error response.

        Args:
            error_code: Error code identifier
            message: Human-readable error message
            details: Additional error details
            original_error: Original exception that caused the error

        Returns:
            MCPError: Structured error object
        """
        if original_error and not details:
            details = {
                "error_type": type(original_error).__name__,
                "original_message": str(original_error),
            }

        error = MCPError(code=error_code, message=message, details=details)

        # Log the error creation
        logger.debug(f"Created MCP error: {error_code} - {message}")

        return error

    def handle_database_error(self, error: Exception) -> MCPError:
        """Handle database-specific errors and convert to MCP format.

        Args:
            error: Database exception

        Returns:
            MCPError: Structured MCP error
        """
        error_message = str(error)

        # Categorize database errors
        if "connection" in error_message.lower():
            return self.create_mcp_error(
                "DATABASE_CONNECTION_ERROR",
                "Failed to connect to database",
                {"connection_error": error_message},
                error,
            )
        elif "syntax error" in error_message.lower():
            return self.create_mcp_error(
                "SQL_SYNTAX_ERROR",
                "SQL query contains syntax errors",
                {"sql_error": error_message},
                error,
            )
        elif "permission denied" in error_message.lower():
            return self.create_mcp_error(
                "PERMISSION_DENIED",
                "Insufficient permissions for database operation",
                {"permission_error": error_message},
                error,
            )
        elif "does not exist" in error_message.lower():
            return self.create_mcp_error(
                "RESOURCE_NOT_FOUND",
                "Requested database resource does not exist",
                {"resource_error": error_message},
                error,
            )
        else:
            return self.create_mcp_error(
                "DATABASE_ERROR",
                "Database operation failed",
                {"database_error": error_message},
                error,
            )

    def handle_validation_error(self, error: Exception) -> MCPError:
        """Handle validation errors and convert to MCP format.

        Args:
            error: Validation exception

        Returns:
            MCPError: Structured MCP error
        """
        return self.create_mcp_error(
            "VALIDATION_ERROR",
            "Input validation failed",
            {"validation_error": str(error)},
            error,
        )

    def handle_security_error(self, error: Exception) -> MCPError:
        """Handle security-related errors and convert to MCP format.

        Args:
            error: Security exception

        Returns:
            MCPError: Structured MCP error
        """
        return self.create_mcp_error(
            "SECURITY_ERROR",
            "Security validation failed",
            {"security_error": str(error)},
            error,
        )

    def handle_generic_error(self, error: Exception) -> MCPError:
        """Handle generic errors and convert to MCP format.

        Args:
            error: Generic exception

        Returns:
            MCPError: Structured MCP error
        """
        return self.create_mcp_error(
            "INTERNAL_ERROR",
            "An internal error occurred",
            {"internal_error": str(error)},
            error,
        )

    def get_execution_stats(self) -> dict[str, Any]:
        """Get execution statistics.

        Returns:
            Dictionary containing execution statistics
        """
        stats = self._execution_stats.copy()
        stats["active_executions"] = len(self._active_contexts)
        stats["success_rate"] = stats["successful_executions"] / max(
            stats["total_executions"], 1
        )
        stats["average_execution_time"] = stats["total_execution_time"] / max(
            stats["successful_executions"], 1
        )
        return stats

    def get_active_contexts(self) -> dict[str, dict[str, Any]]:
        """Get information about currently active tool executions.

        Returns:
            Dictionary of active execution contexts
        """
        return {
            execution_id: {
                "tool_name": context.tool_name,
                "start_time": context.start_time,
                "running_time": time.time() - context.start_time,
                "parameters": context.parameters,
            }
            for execution_id, context in self._active_contexts.items()
        }

    def log_tool_usage(
        self,
        tool_name: str,
        parameters: dict[str, Any],
        success: bool = True,
        execution_time: float | None = None,
        result_size: int | None = None,
    ) -> None:
        """Log tool usage for monitoring and analytics.

        Args:
            tool_name: Name of the tool used
            parameters: Tool parameters
            success: Whether the execution was successful
            execution_time: Execution time in seconds
            result_size: Size of the result (e.g., number of rows)
        """
        log_data = {
            "tool_name": tool_name,
            "success": success,
            "execution_time": execution_time,
            "result_size": result_size,
            "parameter_count": len(parameters),
        }

        if server_config.debug:
            log_data["parameters"] = parameters

        logger.info(f"Tool usage logged: {log_data}")

    def reset_stats(self) -> None:
        """Reset execution statistics."""
        self._execution_stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "failed_executions": 0,
            "total_execution_time": 0.0,
            "tool_usage_count": {},
        }
        logger.info("Execution statistics reset")


# Global context manager instance
mcp_context = MCPContextManager()
