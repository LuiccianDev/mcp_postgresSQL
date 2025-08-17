"""Application configuration settings for MCP Postgres server."""

import os
from dataclasses import dataclass


@dataclass
class ServerConfig:
    """MCP server configuration."""

    port: int = 8000
    host: str = "localhost"
    log_level: str = "INFO"
    debug: bool = False
    query_timeout: int = 30
    max_query_results: int = 10000
    enable_structured_logging: bool = True
    log_query_parameters: bool = False
    log_execution_time: bool = True
    log_result_size: bool = True


@dataclass
class SecurityConfig:
    """Security configuration settings."""

    enable_query_validation: bool = True
    allowed_schemas: list[str] | None = None
    blocked_operations: list[str] | None = None
    max_query_length: int = 10000

    def __post_init__(self) -> None:
        """Initialize default values after dataclass creation."""
        if self.blocked_operations is None:
            self.blocked_operations = ["DROP", "TRUNCATE", "ALTER"]


def load_server_config() -> ServerConfig:
    """Load server configuration from environment variables."""
    return ServerConfig(
        port=int(os.getenv("MCP_SERVER_PORT", "8000")),
        host=os.getenv("MCP_SERVER_HOST", "localhost"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        debug=os.getenv("DEBUG", "false").lower() == "true",
        query_timeout=int(os.getenv("QUERY_TIMEOUT", "30")),
        max_query_results=int(os.getenv("MAX_QUERY_RESULTS", "10000")),
        enable_structured_logging=os.getenv("ENABLE_STRUCTURED_LOGGING", "true").lower()
        == "true",
        log_query_parameters=os.getenv("LOG_QUERY_PARAMETERS", "false").lower()
        == "true",
        log_execution_time=os.getenv("LOG_EXECUTION_TIME", "true").lower() == "true",
        log_result_size=os.getenv("LOG_RESULT_SIZE", "true").lower() == "true",
    )


def load_security_config() -> SecurityConfig:
    """Load security configuration from environment variables."""
    allowed_schemas = os.getenv("ALLOWED_SCHEMAS")
    blocked_operations = os.getenv("BLOCKED_OPERATIONS")

    return SecurityConfig(
        enable_query_validation=os.getenv("ENABLE_QUERY_VALIDATION", "true").lower()
        == "true",
        allowed_schemas=allowed_schemas.split(",") if allowed_schemas else None,
        blocked_operations=blocked_operations.split(",")
        if blocked_operations
        else None,
        max_query_length=int(os.getenv("MAX_QUERY_LENGTH", "10000")),
    )


def validate_environment() -> None:
    """Validate that required environment variables are set."""
    required_vars = ["DATABASE_URL"]
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )


# Global configuration instances
server_config = load_server_config()
security_config = load_security_config()
