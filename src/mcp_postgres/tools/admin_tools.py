"""Database administration tools for MCP Postgres server."""

import logging
import time
from typing import Any

from mcp_postgres.core.connection import connection_manager
from mcp_postgres.core.security import validate_query_permissions
from mcp_postgres.utils.exceptions import (
    SecurityError,
    ValidationError,
    handle_postgres_error,
)
from mcp_postgres.utils.formatters import (
    format_bytes,
    format_duration,
    format_error_response,
    format_success_response,
    serialize_dict,
)


logger = logging.getLogger(__name__)


async def get_database_info() -> dict[str, Any]:
    """Get comprehensive database metadata and connection information.

    This tool retrieves database version, size, connection details, and other
    metadata useful for database administration and monitoring.

    Returns:
        Dictionary containing database information and metadata

    Raises:
        QueryExecutionError: If database queries fail
    """
    try:
        start_time = time.time()

        # Query for database version and basic info
        version_query = "SELECT version() as version"
        version_result = await connection_manager.execute_query(
            version_query, fetch_mode="val"
        )

        # Query for database size
        size_query = """
        SELECT pg_database_size(current_database()) as database_size_bytes
        """
        size_result = await connection_manager.execute_query(
            size_query, fetch_mode="val"
        )

        size_result = size_result if size_result is not None else 0

        # Query for database settings
        settings_query = """
        SELECT
            current_database() as database_name,
            current_user as current_user,
            session_user as session_user,
            current_schema() as current_schema,
            inet_server_addr() as server_address,
            inet_server_port() as server_port,
            pg_backend_pid() as backend_pid,
            pg_is_in_recovery() as is_in_recovery
        """
        settings_result = await connection_manager.execute_query(
            settings_query, fetch_mode="one"
        )

        settings_result = settings_result if settings_result is not None else {}

        # Query for connection limits and current connections
        connections_query = """
        SELECT
            setting as max_connections
        FROM pg_settings
        WHERE name = 'max_connections'
        """
        max_conn_result = await connection_manager.execute_query(
            connections_query, fetch_mode="val"
        )

        max_conn_result = max_conn_result if max_conn_result is not None else 0

        current_conn_query = """
        SELECT count(*) as current_connections
        FROM pg_stat_activity
        WHERE state IS NOT NULL
        """
        current_conn_result = await connection_manager.execute_query(
            current_conn_query, fetch_mode="val"
        )

        current_conn_result = (
            current_conn_result if current_conn_result is not None else 0
        )

        # Query for database statistics
        stats_query = """
        SELECT
            numbackends as active_connections,
            xact_commit as transactions_committed,
            xact_rollback as transactions_rolled_back,
            blks_read as blocks_read,
            blks_hit as blocks_hit,
            tup_returned as tuples_returned,
            tup_fetched as tuples_fetched,
            tup_inserted as tuples_inserted,
            tup_updated as tuples_updated,
            tup_deleted as tuples_deleted
        FROM pg_stat_database
        WHERE datname = current_database()
        """
        stats_result = await connection_manager.execute_query(
            stats_query, fetch_mode="one"
        )

        stats_result = stats_result if stats_result is not None else {}

        execution_time = time.time() - start_time

        # Format the response
        database_info = {
            "version": str(version_result),
            "database_name": settings_result["database_name"],
            "current_user": settings_result["current_user"],
            "session_user": settings_result["session_user"],
            "current_schema": settings_result["current_schema"],
            "server_address": settings_result["server_address"],
            "server_port": settings_result["server_port"],
            "backend_pid": settings_result["backend_pid"],
            "is_in_recovery": settings_result["is_in_recovery"],
            "database_size_bytes": int(size_result),
            "database_size_human": format_bytes(int(size_result)),
            "max_connections": int(max_conn_result),
            "current_connections": int(current_conn_result),
            "connection_usage_percent": round(
                (int(current_conn_result) / int(max_conn_result)) * 100, 2
            ),
            "statistics": serialize_dict(dict(stats_result)),
            "cache_hit_ratio": round(
                (
                    stats_result["blocks_hit"]
                    / max(stats_result["blocks_hit"] + stats_result["blocks_read"], 1)
                )
                * 100,
                2,
            ),
            "execution_time_ms": round(execution_time * 1000, 2),
        }

        logger.info(f"Database info retrieved successfully in {execution_time:.3f}s")

        return format_success_response(
            data=database_info, message="Database information retrieved successfully"
        )

    except Exception as e:
        logger.error(f"Error retrieving database info: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def monitor_connections() -> dict[str, Any]:
    """Monitor active database connections and session information.

    This tool provides detailed information about current database connections,
    including active queries, connection states, and session statistics.

    Returns:
        Dictionary containing connection monitoring data

    Raises:
        QueryExecutionError: If monitoring queries fail
    """
    try:
        start_time = time.time()

        # Query for active connections with detailed information
        connections_query = """
        SELECT
            pid,
            usename as username,
            application_name,
            client_addr as client_address,
            client_port,
            backend_start,
            query_start,
            state_change,
            state,
            query,
            backend_type,
            EXTRACT(EPOCH FROM (now() - backend_start)) as connection_duration_seconds,
            EXTRACT(EPOCH FROM (now() - query_start)) as query_duration_seconds,
            CASE
                WHEN state = 'active' THEN 1
                ELSE 0
            END as is_active
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        ORDER BY backend_start DESC
        """
        connections_result = await connection_manager.execute_query(
            connections_query, fetch_mode="all"
        )

        # Query for connection summary statistics
        summary_query = """
        SELECT
            state,
            count(*) as connection_count,
            avg(EXTRACT(EPOCH FROM (now() - backend_start))) as avg_connection_duration,
            max(EXTRACT(EPOCH FROM (now() - backend_start))) as max_connection_duration
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid()
        GROUP BY state
        ORDER BY connection_count DESC
        """
        summary_result = await connection_manager.execute_query(
            summary_query, fetch_mode="all"
        )

        # Query for long-running queries
        long_queries_query = """
        SELECT
            pid,
            usename as username,
            query,
            state,
            EXTRACT(EPOCH FROM (now() - query_start)) as query_duration_seconds
        FROM pg_stat_activity
        WHERE state = 'active'
        AND query_start IS NOT NULL
        AND EXTRACT(EPOCH FROM (now() - query_start)) > 30
        ORDER BY query_duration_seconds DESC
        LIMIT 10
        """
        long_queries_result = await connection_manager.execute_query(
            long_queries_query, fetch_mode="all"
        )

        # Query for blocked queries
        blocked_queries_query = """
        SELECT
            blocked_locks.pid as blocked_pid,
            blocked_activity.usename as blocked_user,
            blocking_locks.pid as blocking_pid,
            blocking_activity.usename as blocking_user,
            blocked_activity.query as blocked_query,
            blocking_activity.query as blocking_query
        FROM pg_catalog.pg_locks blocked_locks
        JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
            AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
            AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        """
        blocked_queries_result = await connection_manager.execute_query(
            blocked_queries_query, fetch_mode="all"
        )

        execution_time = time.time() - start_time

        # Format connections data
        formatted_connections = []
        for conn in connections_result:
            conn_dict = dict(conn)
            conn_dict["connection_duration_human"] = format_duration(
                conn_dict.get("connection_duration_seconds", 0)
            )
            if conn_dict.get("query_duration_seconds"):
                conn_dict["query_duration_human"] = format_duration(
                    conn_dict["query_duration_seconds"]
                )
            formatted_connections.append(serialize_dict(conn_dict))

        # Format summary data
        formatted_summary = []
        for summary in summary_result:
            summary_dict = dict(summary)
            if summary_dict.get("avg_connection_duration"):
                summary_dict["avg_connection_duration_human"] = format_duration(
                    summary_dict["avg_connection_duration"]
                )
            if summary_dict.get("max_connection_duration"):
                summary_dict["max_connection_duration_human"] = format_duration(
                    summary_dict["max_connection_duration"]
                )
            formatted_summary.append(serialize_dict(summary_dict))

        # Format long-running queries
        formatted_long_queries = []
        for query in long_queries_result:
            query_dict = dict(query)
            query_dict["query_duration_human"] = format_duration(
                query_dict.get("query_duration_seconds", 0)
            )
            formatted_long_queries.append(serialize_dict(query_dict))

        monitoring_data = {
            "connections": formatted_connections,
            "connection_count": len(formatted_connections),
            "summary_by_state": formatted_summary,
            "long_running_queries": formatted_long_queries,
            "blocked_queries": [
                serialize_dict(dict(bq)) for bq in blocked_queries_result
            ],
            "active_connections": sum(
                1 for conn in connections_result if conn["is_active"]
            ),
            "execution_time_ms": round(execution_time * 1000, 2),
            "metadata": {
                "has_long_queries": len(long_queries_result) > 0,
                "has_blocked_queries": len(blocked_queries_result) > 0,
                "monitoring_timestamp": time.time(),
            },
        }

        logger.info(
            f"Connection monitoring completed successfully in {execution_time:.3f}s"
        )

        return format_success_response(
            data=monitoring_data, message="Connection monitoring completed successfully"
        )

    except Exception as e:
        logger.error(f"Error monitoring connections: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def vacuum_table(
    table_name: str, analyze: bool = True, full: bool = False
) -> dict[str, Any]:
    """Perform VACUUM operation on a table for maintenance.

    This tool executes VACUUM on the specified table to reclaim storage space
    and update table statistics. Can optionally perform ANALYZE and FULL vacuum.

    Args:
        table_name: Name of the table to vacuum
        analyze: Whether to run ANALYZE after vacuum (default: True)
        full: Whether to perform VACUUM FULL (default: False)

    Returns:
        Dictionary containing vacuum operation results

    Raises:
        ValidationError: If table name is invalid
        SecurityError: If operation fails security validation
        QueryExecutionError: If vacuum operation fails
    """
    try:
        # Validate inputs
        if not table_name or not table_name.strip():
            raise ValidationError("Table name cannot be empty")

        # Sanitize table name (basic validation)
        table_name = table_name.strip()
        if not table_name.replace("_", "").replace(".", "").isalnum():
            raise ValidationError("Table name contains invalid characters")

        start_time = time.time()

        # Check if table exists first
        table_check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = $1
            AND table_schema = current_schema()
        )
        """
        table_exists = await connection_manager.execute_query(
            table_check_query, [table_name], fetch_mode="val"
        )

        if not table_exists:
            raise ValidationError(f"Table '{table_name}' does not exist")

        # Get table size before vacuum
        size_before_query = """
        SELECT pg_total_relation_size($1) as size_bytes
        """
        size_before = await connection_manager.execute_query(
            size_before_query, [table_name], fetch_mode="val"
        )

        # Build vacuum command
        vacuum_options = []
        if analyze:
            vacuum_options.append("ANALYZE")
        if full:
            vacuum_options.append("FULL")

        vacuum_command = f"VACUUM {' '.join(vacuum_options)} {table_name}"

        # Security validation for the vacuum command
        is_valid, error_msg = validate_query_permissions(vacuum_command)
        if not is_valid:
            raise SecurityError(
                f"Vacuum operation security validation failed: {error_msg}"
            )

        logger.info(
            f"Starting vacuum operation on table '{table_name}': {vacuum_command}"
        )

        # Execute vacuum (this is a maintenance command, so fetch_mode="none")
        vacuum_result = await connection_manager.execute_raw_query(
            vacuum_command, fetch_mode="none"
        )

        # Get table size after vacuum
        size_after = await connection_manager.execute_query(
            size_before_query, [table_name], fetch_mode="val"
        )

        size_before = size_before if size_before is not None else 0
        size_after = size_after if size_after is not None else 0

        # Get table statistics
        stats_query = """
        SELECT
            schemaname,
            tablename,
            n_tup_ins as tuples_inserted,
            n_tup_upd as tuples_updated,
            n_tup_del as tuples_deleted,
            n_live_tup as live_tuples,
            n_dead_tup as dead_tuples,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze,
            vacuum_count,
            autovacuum_count,
            analyze_count,
            autoanalyze_count
        FROM pg_stat_user_tables
        WHERE tablename = $1
        """
        stats_result = await connection_manager.execute_query(
            stats_query, [table_name], fetch_mode="one"
        )

        execution_time = time.time() - start_time

        # Calculate space reclaimed
        space_reclaimed = int(size_before) - int(size_after)

        vacuum_data = {
            "table_name": table_name,
            "vacuum_type": "FULL" if full else "STANDARD",
            "analyze_performed": analyze,
            "operation_status": str(vacuum_result),
            "size_before_bytes": int(size_before),
            "size_after_bytes": int(size_after),
            "size_before_human": format_bytes(int(size_before)),
            "size_after_human": format_bytes(int(size_after)),
            "space_reclaimed_bytes": space_reclaimed,
            "space_reclaimed_human": format_bytes(abs(space_reclaimed)),
            "space_reclaimed_percent": round(
                (space_reclaimed / max(int(size_before), 1)) * 100, 2
            )
            if size_before
            else 0,
            "execution_time_ms": round(execution_time * 1000, 2),
            "execution_time_human": format_duration(execution_time),
            "table_statistics": serialize_dict(dict(stats_result))
            if stats_result
            else None,
        }

        logger.info(
            f"Vacuum operation completed successfully on table '{table_name}' in {execution_time:.3f}s"
        )

        return format_success_response(
            data=vacuum_data,
            message=f"Vacuum operation completed successfully on table '{table_name}'",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Vacuum operation validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Error during vacuum operation: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def reindex_table(
    table_name: str, index_name: str | None = None
) -> dict[str, Any]:
    """Rebuild indexes for a table or specific index.

    This tool rebuilds indexes to improve query performance and reclaim space
    from bloated indexes. Can rebuild all indexes for a table or a specific index.

    Args:
        table_name: Name of the table to reindex
        index_name: Optional specific index name to rebuild

    Returns:
        Dictionary containing reindex operation results

    Raises:
        ValidationError: If table/index name is invalid
        SecurityError: If operation fails security validation
        QueryExecutionError: If reindex operation fails
    """
    try:
        # Validate inputs
        if not table_name or not table_name.strip():
            raise ValidationError("Table name cannot be empty")

        # Sanitize table name
        table_name = table_name.strip()
        if not table_name.replace("_", "").replace(".", "").isalnum():
            raise ValidationError("Table name contains invalid characters")

        if index_name:
            index_name = index_name.strip()
            if not index_name.replace("_", "").replace(".", "").isalnum():
                raise ValidationError("Index name contains invalid characters")

        start_time = time.time()

        # Check if table exists
        table_check_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = $1
            AND table_schema = current_schema()
        )
        """
        table_exists = await connection_manager.execute_query(
            table_check_query, [table_name], fetch_mode="val"
        )

        if not table_exists:
            raise ValidationError(f"Table '{table_name}' does not exist")

        # If specific index is requested, check if it exists
        if index_name:
            index_check_query = """
            SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE tablename = $1
                AND indexname = $2
                AND schemaname = current_schema()
            )
            """
            index_exists = await connection_manager.execute_query(
                index_check_query, [table_name, index_name], fetch_mode="val"
            )

            if not index_exists:
                raise ValidationError(
                    f"Index '{index_name}' does not exist on table '{table_name}'"
                )

        # Get index information before reindex
        indexes_query = """
        SELECT
            indexname,
            indexdef,
            pg_relation_size(indexname::regclass) as index_size_bytes
        FROM pg_indexes
        WHERE tablename = $1
        AND schemaname = current_schema()
        """
        if index_name:
            indexes_query += " AND indexname = $2"
            indexes_before = await connection_manager.execute_query(
                indexes_query, [table_name, index_name], fetch_mode="all"
            )
        else:
            indexes_before = await connection_manager.execute_query(
                indexes_query, [table_name], fetch_mode="all"
            )

        # Build reindex command
        if index_name:
            reindex_command = f"REINDEX INDEX {index_name}"
        else:
            reindex_command = f"REINDEX TABLE {table_name}"

        # Security validation
        is_valid, error_msg = validate_query_permissions(reindex_command)
        if not is_valid:
            raise SecurityError(
                f"Reindex operation security validation failed: {error_msg}"
            )

        logger.info(f"Starting reindex operation: {reindex_command}")

        # Execute reindex
        reindex_result = await connection_manager.execute_raw_query(
            reindex_command, fetch_mode="none"
        )

        # Get index information after reindex
        if index_name:
            indexes_after = await connection_manager.execute_query(
                indexes_query, [table_name, index_name], fetch_mode="all"
            )
        else:
            indexes_after = await connection_manager.execute_query(
                indexes_query, [table_name], fetch_mode="all"
            )

        execution_time = time.time() - start_time

        # Calculate size changes
        total_size_before = sum(idx["index_size_bytes"] for idx in indexes_before)
        total_size_after = sum(idx["index_size_bytes"] for idx in indexes_after)
        size_change = total_size_after - total_size_before

        # Format index information
        formatted_indexes_before = [serialize_dict(dict(idx)) for idx in indexes_before]
        formatted_indexes_after = [serialize_dict(dict(idx)) for idx in indexes_after]

        reindex_data = {
            "table_name": table_name,
            "index_name": index_name,
            "operation_type": "INDEX" if index_name else "TABLE",
            "operation_status": str(reindex_result),
            "indexes_processed": len(indexes_before),
            "total_size_before_bytes": total_size_before,
            "total_size_after_bytes": total_size_after,
            "total_size_before_human": format_bytes(total_size_before),
            "total_size_after_human": format_bytes(total_size_after),
            "size_change_bytes": size_change,
            "size_change_human": format_bytes(abs(size_change)),
            "size_change_percent": round(
                (size_change / max(total_size_before, 1)) * 100, 2
            )
            if total_size_before
            else 0,
            "execution_time_ms": round(execution_time * 1000, 2),
            "execution_time_human": format_duration(execution_time),
            "indexes_before": formatted_indexes_before,
            "indexes_after": formatted_indexes_after,
        }

        logger.info(
            f"Reindex operation completed successfully in {execution_time:.3f}s"
        )

        return format_success_response(
            data=reindex_data, message="Reindex operation completed successfully"
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Reindex operation validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Error during reindex operation: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


# Tool schema definitions for MCP registration
GET_DATABASE_INFO_SCHEMA = {
    "name": "get_database_info",
    "description": "Get comprehensive database metadata including version, size, connections, and statistics",
    "inputSchema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

MONITOR_CONNECTIONS_SCHEMA = {
    "name": "monitor_connections",
    "description": "Monitor active database connections, sessions, and identify long-running or blocked queries",
    "inputSchema": {
        "type": "object",
        "properties": {},
        "required": [],
    },
}

VACUUM_TABLE_SCHEMA = {
    "name": "vacuum_table",
    "description": "Perform VACUUM operation on a table to reclaim storage space and update statistics",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to vacuum",
            },
            "analyze": {
                "type": "boolean",
                "description": "Whether to run ANALYZE after vacuum to update table statistics",
                "default": True,
            },
            "full": {
                "type": "boolean",
                "description": "Whether to perform VACUUM FULL (more thorough but locks table)",
                "default": False,
            },
        },
        "required": ["table_name"],
    },
}

REINDEX_TABLE_SCHEMA = {
    "name": "reindex_table",
    "description": "Rebuild indexes for a table or specific index to improve performance and reclaim space",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to reindex",
            },
            "index_name": {
                "type": "string",
                "description": "Optional specific index name to rebuild (if not provided, all table indexes are rebuilt)",
            },
        },
        "required": ["table_name"],
    },
}

# Export tool functions and schemas
__all__ = [
    "get_database_info",
    "monitor_connections",
    "vacuum_table",
    "reindex_table",
    "GET_DATABASE_INFO_SCHEMA",
    "MONITOR_CONNECTIONS_SCHEMA",
    "VACUUM_TABLE_SCHEMA",
    "REINDEX_TABLE_SCHEMA",
]
