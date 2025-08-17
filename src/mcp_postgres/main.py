"""Main entry point for MCP Postgres server."""

import argparse
import asyncio
import logging
import signal
import sys
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server

from .config.database import database_config, validate_database_config
from .config.settings import server_config, validate_environment
from .core.connection import connection_manager
from .tools.register_tools import register_all_tools


def setup_logging() -> None:
    """Configure logging for the MCP server."""
    logging.basicConfig(
        level=getattr(logging, server_config.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


async def shutdown_handler(server: Server) -> None:
    """Handle graceful shutdown of the server."""
    logger = logging.getLogger(__name__)
    logger.info("Shutting down MCP Postgres server...")

    # Close database connection pool
    try:
        await connection_manager.close()
        logger.info("Database connection pool closed")
    except Exception as e:
        logger.error(f"Error closing connection pool: {e}")

    logger.info("MCP Postgres server shutdown complete")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="MCP Postgres Server - PostgreSQL database interaction via MCP protocol"
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Enable development mode with debug logging"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (overrides LOG_LEVEL environment variable)"
    )
    parser.add_argument(
        "--version",
        action="version",
        version="mcp-postgres 0.1.0"
    )
    return parser.parse_args()


async def main() -> None:
    """Main entry point for the MCP Postgres server."""
    # Parse command line arguments
    args = parse_args()

    # Override environment settings with CLI args
    if args.dev:
        import os
        os.environ["LOG_LEVEL"] = "DEBUG"
        os.environ["DEV_MODE"] = "true"

    if args.log_level:
        import os
        os.environ["LOG_LEVEL"] = args.log_level

    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)

    if args.dev:
        logger.info("Development mode enabled")

    try:
        # Validate environment and configuration
        validate_environment()
        validate_database_config(database_config)

        logger.info("Starting MCP Postgres server...")
        logger.info(f"Database host: {database_config.host}:{database_config.port}")
        logger.info(f"Database name: {database_config.database}")
        logger.info(f"Pool size: {database_config.pool_size}")

        # Initialize database connection pool
        logger.info("Initializing database connection pool...")
        await connection_manager.initialize()

        # Perform health check
        health_status = await connection_manager.health_check()
        if health_status["status"] != "healthy":
            raise ConnectionError(f"Database health check failed: {health_status.get('error', 'Unknown error')}")

        logger.info("Database connection pool initialized successfully")
        logger.info(f"Pool stats: {health_status['pool_stats']}")

        # Create MCP server instance
        server = Server("mcp-postgres")

        # Register all tools with the MCP server
        await register_all_tools(server)
        logger.info("All tools registered successfully")

        # Setup signal handlers for graceful shutdown
        def signal_handler(signum: int, frame: Any) -> None:
            logger.info(f"Received signal {signum}, initiating shutdown...")
            asyncio.create_task(shutdown_handler(server))

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.info("MCP Postgres server is ready to accept connections")

        # Run the server
        async with stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options()
            )

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        await shutdown_handler(server)
    except Exception as e:
        logger.error(f"Failed to start MCP Postgres server: {e}")
        # Ensure connection pool is closed on error
        try:
            await connection_manager.close()
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
