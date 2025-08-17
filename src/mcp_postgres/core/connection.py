"""Database connection management for MCP Postgres server."""

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Literal, overload

import asyncpg
from asyncpg import Connection, Pool, Record

from ..config.database import DatabaseConfig, database_config


logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages PostgreSQL connection pool and query execution."""

    def __init__(self, config: DatabaseConfig):
        """Initialize connection manager with database configuration.

        Args:
            config: Database configuration instance
        """
        self.config = config
        self._pool: Pool | None = None
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Initialize the connection pool."""
        async with self._lock:
            if self._pool is not None:
                logger.warning("Connection pool already initialized")
                return

            try:
                logger.info(
                    f"Creating connection pool to {self.config.host}:{self.config.port}/{self.config.database}"
                )
                self._pool = await asyncpg.create_pool(**self.config.pool_kwargs)
                logger.info(
                    f"Connection pool created successfully with {self.config.pool_size} max connections"
                )
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise ConnectionError(
                    f"Failed to initialize database connection pool: {e}"
                ) from e

    async def close(self) -> None:
        """Close the connection pool."""
        async with self._lock:
            if self._pool is not None:
                logger.info("Closing connection pool")
                await self._pool.close()
                self._pool = None
                logger.info("Connection pool closed")

    @property
    def is_initialized(self) -> bool:
        """Check if connection pool is initialized."""
        return self._pool is not None

    async def get_connection(self) -> Connection:
        """Get a connection from the pool.

        Returns:
            Database connection from the pool

        Raises:
            ConnectionError: If pool is not initialized or connection fails
        """
        if self._pool is None:
            raise ConnectionError(
                "Connection pool not initialized. Call initialize() first."
            )

        try:
            return await self._pool.acquire()
        except Exception as e:
            logger.error(f"Failed to acquire connection from pool: {e}")
            raise ConnectionError(f"Failed to get database connection: {e}") from e

    async def release_connection(self, connection: Connection) -> None:
        """Release a connection back to the pool.

        Args:
            connection: Connection to release
        """
        if self._pool is not None:
            try:
                await self._pool.release(connection)
            except Exception as e:
                logger.error(f"Failed to release connection: {e}")

    @asynccontextmanager
    async def connection(self) -> AsyncGenerator[Connection]:
        """Context manager for database connections.

        Yields:
            Database connection that is automatically released
        """
        conn = await self.get_connection()
        try:
            yield conn
        finally:
            await self.release_connection(conn)

    @overload
    async def execute_query(
        self, query: str, parameters: list[Any] | None = None, fetch_mode: Literal["all"] = "all"
    ) -> list[Record]: ...

    @overload
    async def execute_query(
        self, query: str, parameters: list[Any] | None = None, fetch_mode: Literal["one"] = "one"
    ) -> Record | None: ...

    @overload
    async def execute_query(
        self, query: str, parameters: list[Any] | None = None, fetch_mode: Literal["val"] = "val"
    ) -> int | float | str | None: ...

    @overload
    async def execute_query(
        self, query: str, parameters: list[Any] | None = None, fetch_mode: Literal["none"] = "none"
    ) -> str: ...

    async def execute_query(
        self, query: str, parameters: list[Any] | None = None, fetch_mode: str = "all"
    ) -> list[Record] | Record | str | int | float | None:
        """Execute a parameterized SQL query.

        Args:
            query: SQL query with parameter placeholders ($1, $2, etc.)
            parameters: Query parameters to bind
            fetch_mode: How to fetch results ('all', 'one', 'none', 'val')

        Returns:
            Query results based on fetch_mode:
            - 'all': List of Record objects
            - 'one': Single Record object or None
            - 'none': Status string for non-SELECT queries
            - 'val': Single value from first row/column

        Raises:
            ValueError: If query or parameters are invalid
            ConnectionError: If database connection fails
            Exception: For SQL execution errors
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        parameters = parameters or []

        async with self.connection() as conn:
            try:
                logger.debug(
                    f"Executing query: {query[:100]}{'...' if len(query) > 100 else ''}"
                )
                logger.debug(f"Parameters: {parameters}")

                if fetch_mode == "all":
                    result = await conn.fetch(query, *parameters)
                elif fetch_mode == "one":
                    result = await conn.fetchrow(query, *parameters)
                elif fetch_mode == "val":
                    result = await conn.fetchval(query, *parameters)
                elif fetch_mode == "none":
                    result = await conn.execute(query, *parameters)
                else:
                    raise ValueError(f"Invalid fetch_mode: {fetch_mode}")

                logger.debug(f"Query executed successfully, fetch_mode: {fetch_mode}")
                return result

            except asyncpg.PostgresError as e:
                logger.error(f"PostgreSQL error executing query: {e}")
                raise Exception(f"Database query failed: {e}") from e
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                raise

    async def execute_raw_query(
        self, query: str, fetch_mode: str = "all"
    ) -> list[Record] | Record | str | int:
        """Execute a raw SQL query without parameter binding.

        WARNING: This method does not use parameter binding and may be vulnerable
        to SQL injection if used with untrusted input. Use execute_query() instead
        when possible.

        Args:
            query: Raw SQL query
            fetch_mode: How to fetch results ('all', 'one', 'none', 'val')

        Returns:
            Query results based on fetch_mode

        Raises:
            ValueError: If query is invalid
            ConnectionError: If database connection fails
            Exception: For SQL execution errors
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        logger.warning(
            f"Executing raw query (potential SQL injection risk): {query[:100]}{'...' if len(query) > 100 else ''}"
        )

        async with self.connection() as conn:
            try:
                if fetch_mode == "all":
                    result = await conn.fetch(query)
                elif fetch_mode == "one":
                    result = await conn.fetchrow(query)
                elif fetch_mode == "val":
                    result = await conn.fetchval(query)
                elif fetch_mode == "none":
                    result = await conn.execute(query)
                else:
                    raise ValueError(f"Invalid fetch_mode: {fetch_mode}")

                logger.debug(
                    f"Raw query executed successfully, fetch_mode: {fetch_mode}"
                )
                return result

            except asyncpg.PostgresError as e:
                logger.error(f"PostgreSQL error executing raw query: {e}")
                raise Exception(f"Database query failed: {e}") from e
            except Exception as e:
                logger.error(f"Unexpected error executing raw query: {e}")
                raise

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Connection]:
        """Context manager for database transactions.

        Automatically handles transaction commit/rollback.

        Yields:
            Database connection within a transaction context

        Example:
            async with connection_manager.transaction() as conn:
                await conn.execute("INSERT INTO users (name) VALUES ($1)", "John")
                await conn.execute("INSERT INTO profiles (user_id) VALUES ($1)", user_id)
                # Transaction is automatically committed
        """
        async with self.connection() as conn:
            async with conn.transaction():
                yield conn

    async def execute_transaction(
        self, queries: list[dict[str, Any]]
    ) -> list[list[Record] | Record | str | int]:
        """Execute multiple queries in a single transaction.

        Args:
            queries: List of query dictionaries with 'query', 'parameters', and 'fetch_mode' keys

        Returns:
            List of results for each query

        Raises:
            ValueError: If queries format is invalid
            Exception: For transaction execution errors

        Example:
            queries = [
                {"query": "INSERT INTO users (name) VALUES ($1)", "parameters": ["John"], "fetch_mode": "none"},
                {"query": "SELECT id FROM users WHERE name = $1", "parameters": ["John"], "fetch_mode": "val"}
            ]
            results = await connection_manager.execute_transaction(queries)
        """
        if not queries:
            raise ValueError("Queries list cannot be empty")

        results = []

        async with self.transaction() as conn:
            for i, query_info in enumerate(queries):
                if not isinstance(query_info, dict):
                    raise ValueError(f"Query {i} must be a dictionary")

                query = query_info.get("query")
                parameters = query_info.get("parameters", [])
                fetch_mode = query_info.get("fetch_mode", "all")

                if not query:
                    raise ValueError(f"Query {i} is missing 'query' field")

                try:
                    logger.debug(
                        f"Executing transaction query {i}: {query[:100]}{'...' if len(query) > 100 else ''}"
                    )

                    if fetch_mode == "all":
                        result = await conn.fetch(query, *parameters)
                    elif fetch_mode == "one":
                        result = await conn.fetchrow(query, *parameters)
                    elif fetch_mode == "val":
                        result = await conn.fetchval(query, *parameters)
                    elif fetch_mode == "none":
                        result = await conn.execute(query, *parameters)
                    else:
                        raise ValueError(
                            f"Invalid fetch_mode in query {i}: {fetch_mode}"
                        )

                    results.append(result)

                except Exception as e:
                    logger.error(f"Error executing query {i} in transaction: {e}")
                    raise Exception(f"Transaction failed at query {i}: {e}") from e

        logger.info(f"Transaction completed successfully with {len(queries)} queries")
        return results

    async def health_check(self) -> dict[str, Any]:
        """Perform a health check on the database connection.

        Returns:
            Dictionary with health check results
        """
        if not self.is_initialized:
            return {"status": "unhealthy", "error": "Connection pool not initialized"}

        try:
            async with self.connection() as conn:
                # Simple query to test connection
                result = await conn.fetchval("SELECT 1")

                # Get pool statistics (we know _pool is not None here since connection() succeeded)
                assert self._pool is not None
                pool_stats = {
                    "size": self._pool.get_size(),
                    "max_size": self._pool.get_max_size(),
                    "min_size": self._pool.get_min_size(),
                }

                return {
                    "status": "healthy",
                    "test_query_result": result,
                    "pool_stats": pool_stats,
                    "database": self.config.database,
                    "host": self.config.host,
                    "port": self.config.port,
                }

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"status": "unhealthy", "error": str(e)}


# Global connection manager instance
connection_manager = ConnectionManager(database_config)
