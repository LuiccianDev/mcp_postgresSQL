"""Enhanced logging utilities for MCP Postgres server.

This module provides structured logging capabilities with different levels,
performance metrics, and error tracking for comprehensive monitoring.
"""

import json
import logging
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from typing import Any
from uuid import uuid4

from ..config.settings import server_config


@dataclass
class LogContext:
    """Context information for structured logging."""

    request_id: str = field(default_factory=lambda: str(uuid4())[:8])
    tool_name: str | None = None
    operation: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    start_time: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert context to dictionary for logging."""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class PerformanceMetrics:
    """Performance metrics for logging."""

    execution_time_ms: float
    query_count: int = 0
    result_size: int = 0
    memory_usage_mb: float | None = None
    cpu_time_ms: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary for logging."""
        return {k: v for k, v in asdict(self).items() if v is not None}


class StructuredLogger:
    """Enhanced logger with structured logging capabilities."""

    def __init__(self, name: str):
        """Initialize structured logger.

        Args:
            name: Logger name (typically module name)
        """
        self.logger = logging.getLogger(name)
        self._context_stack: list[LogContext] = []

    def _format_message(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> str:
        """Format message with structured data.

        Args:
            message: Base log message
            context: Log context information
            extra_data: Additional data to include

        Returns:
            Formatted message string
        """
        if not server_config.enable_structured_logging:
            return message

        log_data: dict[str, Any] = {"message": message}

        # Add context information
        if context:
            log_data.update(context.to_dict())
        elif self._context_stack:
            log_data.update(self._context_stack[-1].to_dict())

        # Add extra data
        if extra_data:
            log_data["data"] = extra_data

        # Add timestamp
        log_data["timestamp"] = time.time()

        try:
            return json.dumps(log_data, default=str, separators=(",", ":"))
        except (TypeError, ValueError):
            # Fallback to simple message if JSON serialization fails
            return f"{message} | Context: {context.to_dict() if context else 'None'}"

    def debug(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log debug message with structured data."""
        formatted_msg = self._format_message(message, context, extra_data)
        self.logger.debug(formatted_msg)

    def info(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log info message with structured data."""
        formatted_msg = self._format_message(message, context, extra_data)
        self.logger.info(formatted_msg)

    def warning(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
    ) -> None:
        """Log warning message with structured data."""
        formatted_msg = self._format_message(message, context, extra_data)
        self.logger.warning(formatted_msg)

    def error(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
        exc_info: bool = False,
    ) -> None:
        """Log error message with structured data."""
        formatted_msg = self._format_message(message, context, extra_data)
        self.logger.error(formatted_msg, exc_info=exc_info)

    def critical(
        self,
        message: str,
        context: LogContext | None = None,
        extra_data: dict[str, Any] | None = None,
        exc_info: bool = False,
    ) -> None:
        """Log critical message with structured data."""
        formatted_msg = self._format_message(message, context, extra_data)
        self.logger.critical(formatted_msg, exc_info=exc_info)

    def log_performance(
        self,
        operation: str,
        metrics: PerformanceMetrics,
        context: LogContext | None = None,
    ) -> None:
        """Log performance metrics.

        Args:
            operation: Operation name
            metrics: Performance metrics
            context: Log context
        """
        if not server_config.log_execution_time:
            return

        extra_data = {"operation": operation, "performance": metrics.to_dict()}

        # Determine log level based on execution time
        if metrics.execution_time_ms > 5000:  # > 5 seconds
            self.warning(f"Slow operation detected: {operation}", context, extra_data)
        elif metrics.execution_time_ms > 1000:  # > 1 second
            self.info(f"Operation completed: {operation}", context, extra_data)
        else:
            self.debug(f"Operation completed: {operation}", context, extra_data)

    def log_query(
        self,
        query: str,
        parameters: list[Any] | None = None,
        execution_time_ms: float | None = None,
        result_count: int | None = None,
        context: LogContext | None = None,
    ) -> None:
        """Log database query execution.

        Args:
            query: SQL query
            parameters: Query parameters
            execution_time_ms: Execution time in milliseconds
            result_count: Number of results returned
            context: Log context
        """
        extra_data: dict[str, Any] = {
            "query_length": len(query),
            "has_parameters": parameters is not None and len(parameters) > 0,
        }

        # Add query text in debug mode or if explicitly enabled
        if server_config.debug or server_config.log_query_parameters:
            extra_data["query"] = query
            if parameters and server_config.log_query_parameters:
                extra_data["parameters"] = parameters

        # Add performance metrics if available
        if execution_time_ms is not None:
            extra_data["execution_time_ms"] = execution_time_ms

        if result_count is not None and server_config.log_result_size:
            extra_data["result_count"] = result_count

        self.debug("Database query executed", context, extra_data)

    def log_error(
        self,
        error: Exception,
        operation: str | None = None,
        context: LogContext | None = None,
        additional_data: dict[str, Any] | None = None,
    ) -> None:
        """Log error with comprehensive information.

        Args:
            error: Exception that occurred
            operation: Operation that failed
            context: Log context
            additional_data: Additional error context
        """
        extra_data: dict[str, Any] = {
            "error_type": type(error).__name__,
            "error_message": str(error),
        }

        if operation:
            extra_data["failed_operation"] = operation

        if additional_data:
            extra_data.update(additional_data)

        # Add exception details if available
        if hasattr(error, "error_code"):
            extra_data["error_code"] = error.error_code

        if hasattr(error, "details"):
            extra_data["error_details"] = error.details

        message = f"Operation failed: {operation}" if operation else "Error occurred"
        self.error(message, context, extra_data, exc_info=True)

    @contextmanager
    def log_context(self, context: LogContext) -> Generator[LogContext]:
        """Context manager for maintaining log context.

        Args:
            context: Log context to maintain

        Yields:
            The log context
        """
        self._context_stack.append(context)
        try:
            yield context
        finally:
            self._context_stack.pop()


class LoggerFactory:
    """Factory for creating structured loggers."""

    _loggers: dict[str, StructuredLogger] = {}

    @classmethod
    def get_logger(cls, name: str) -> StructuredLogger:
        """Get or create a structured logger.

        Args:
            name: Logger name

        Returns:
            StructuredLogger instance
        """
        if name not in cls._loggers:
            cls._loggers[name] = StructuredLogger(name)
        return cls._loggers[name]


def setup_enhanced_logging() -> None:
    """Setup enhanced logging configuration for the entire application."""
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, server_config.log_level))

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler - use stderr for MCP compatibility
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(getattr(logging, server_config.log_level))

    # Create formatter
    if server_config.enable_structured_logging:
        # Minimal formatter for structured logs (JSON contains all info)
        formatter = logging.Formatter("%(message)s")
    else:
        # Traditional formatter for non-structured logs
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Set specific logger levels
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("mcp").setLevel(logging.INFO)

    # Log configuration
    logger = LoggerFactory.get_logger(__name__)
    logger.info(
        "Enhanced logging configured",
        extra_data={
            "log_level": server_config.log_level,
            "structured_logging": server_config.enable_structured_logging,
            "debug_mode": server_config.debug,
        },
    )


# Convenience function for getting loggers
def get_logger(name: str) -> StructuredLogger:
    """Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)

    Returns:
        StructuredLogger instance
    """
    return LoggerFactory.get_logger(name)
