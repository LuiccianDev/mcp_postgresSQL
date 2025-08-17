"""Database configuration for MCP Postgres server."""

import os
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class DatabaseConfig:
    """PostgreSQL database configuration."""

    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    ssl_mode: str = "prefer"

    @property
    def connection_url(self) -> str:
        """Generate connection URL for asyncpg."""
        return (
            f"postgresql://{self.username}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    @property
    def pool_kwargs(self) -> dict:
        """Generate kwargs for asyncpg.create_pool()."""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.username,
            "password": self.password,
            "min_size": 1,
            "max_size": self.pool_size,
            "command_timeout": self.pool_timeout,
            "server_settings": {
                "application_name": "mcp_postgres_server"
            }
        }


def parse_database_url(database_url: str) -> DatabaseConfig:
    """Parse DATABASE_URL environment variable into DatabaseConfig."""
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is required")

    try:
        parsed = urlparse(database_url)

        if parsed.scheme not in ("postgresql", "postgres"):
            raise ValueError("DATABASE_URL must use postgresql:// or postgres:// scheme")

        return DatabaseConfig(
            host=parsed.hostname or "localhost",
            port=parsed.port or 5432,
            database=parsed.path.lstrip("/") if parsed.path else "",
            username=parsed.username or "",
            password=parsed.password or "",
            pool_size=int(os.getenv("CONNECTION_POOL_SIZE", "10")),
            max_overflow=int(os.getenv("CONNECTION_POOL_MAX_OVERFLOW", "20")),
            pool_timeout=int(os.getenv("CONNECTION_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("CONNECTION_POOL_RECYCLE", "3600")),
            ssl_mode=os.getenv("DATABASE_SSL_MODE", "prefer"),
        )

    except Exception as e:
        raise ValueError(f"Invalid DATABASE_URL format: {e}") from e


def load_database_config() -> DatabaseConfig:
    """Load database configuration from environment variables."""
    database_url = os.getenv("DATABASE_URL")

    if database_url:
        return parse_database_url(database_url)

    # Fallback to individual environment variables
    return DatabaseConfig(
        host=os.getenv("DATABASE_HOST", "localhost"),
        port=int(os.getenv("DATABASE_PORT", "5432")),
        database=os.getenv("DATABASE_NAME", ""),
        username=os.getenv("DATABASE_USER", ""),
        password=os.getenv("DATABASE_PASSWORD", ""),
        pool_size=int(os.getenv("CONNECTION_POOL_SIZE", "10")),
        max_overflow=int(os.getenv("CONNECTION_POOL_MAX_OVERFLOW", "20")),
        pool_timeout=int(os.getenv("CONNECTION_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("CONNECTION_POOL_RECYCLE", "3600")),
        ssl_mode=os.getenv("DATABASE_SSL_MODE", "prefer"),
    )


def validate_database_config(config: DatabaseConfig) -> None:
    """Validate database configuration parameters."""
    errors = []

    if not config.host:
        errors.append("Database host is required")

    if not config.database:
        errors.append("Database name is required")

    if not config.username:
        errors.append("Database username is required")

    if config.port <= 0 or config.port > 65535:
        errors.append("Database port must be between 1 and 65535")

    if config.pool_size <= 0:
        errors.append("Connection pool size must be greater than 0")

    if config.pool_timeout <= 0:
        errors.append("Connection pool timeout must be greater than 0")

    if errors:
        raise ValueError(f"Database configuration errors: {'; '.join(errors)}")


# Global database configuration instance
database_config = load_database_config()
