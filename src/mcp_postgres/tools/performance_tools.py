"""Performance analysis tools for MCP Postgres server."""

import logging
import time
from typing import Any

from mcp_postgres.core.connection import connection_manager
from mcp_postgres.core.security import sanitize_parameters, validate_query_permissions
from mcp_postgres.utils.exceptions import (
    SecurityError,
    ValidationError,
    handle_postgres_error,
)
from mcp_postgres.utils.formatters import (
    format_error_response,
    format_success_response,
    serialize_value,
)


logger = logging.getLogger(__name__)


async def analyze_query_performance(
    query: str, parameters: list[Any] | None = None
) -> dict[str, Any]:
    """Analyze query performance and provide execution plan analysis.

    This tool executes EXPLAIN ANALYZE on the provided query to get detailed
    execution statistics including execution time, row counts, and query plan.

    Args:
        query: SQL query to analyze
        parameters: List of parameters to bind to the query

    Returns:
        Dictionary containing execution plan, performance metrics, and analysis

    Raises:
        ValidationError: If query or parameters are invalid
        SecurityError: If query fails security validation
        QueryExecutionError: If query analysis fails
    """
    try:
        # Validate inputs
        if not query or not query.strip():
            raise ValidationError("Query cannot be empty")

        # Security validation
        is_valid, error_msg = validate_query_permissions(query)
        if not is_valid:
            raise SecurityError(f"Query security validation failed: {error_msg}")

        # Sanitize parameters
        clean_parameters = sanitize_parameters(parameters or [])

        # Record start time
        start_time = time.time()

        # Create EXPLAIN ANALYZE query
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}"

        # Execute the explain query
        result = await connection_manager.execute_query(
            query=explain_query, parameters=clean_parameters, fetch_mode="all"
        )

        execution_time = time.time() - start_time

        # Parse the JSON result
        if result and isinstance(result, list) and len(result) > 0:
            plan_data = result[0]

            # Extract key metrics from the plan
            plan = plan_data.get("Plan", {})
            total_cost = plan.get("Total Cost", 0)
            actual_time = plan.get("Actual Total Time", 0)
            actual_rows = plan.get("Actual Rows", 0)
            planned_rows = plan.get("Plan Rows", 0)

            # Calculate efficiency metrics
            row_accuracy = (
                (min(actual_rows, planned_rows) / max(actual_rows, planned_rows, 1))
                * 100
                if actual_rows > 0 or planned_rows > 0
                else 100
            )

            performance_analysis: dict[str, Any] = {
                "execution_plan": plan_data,
                "performance_metrics": {
                    "total_cost": total_cost,
                    "actual_execution_time_ms": actual_time,
                    "actual_rows": actual_rows,
                    "planned_rows": planned_rows,
                    "row_estimate_accuracy_percent": round(row_accuracy, 2),
                    "analysis_time_ms": round(execution_time * 1000, 2),
                },
                "recommendations": _generate_performance_recommendations(plan),
                "metadata": {
                    "query_analyzed": query[:100] + ("..." if len(query) > 100 else ""),
                    "has_parameters": len(clean_parameters) > 0,
                    "parameter_count": len(clean_parameters),
                },
            }
        else:
            performance_analysis = {
                "execution_plan": None,
                "performance_metrics": {
                    "analysis_time_ms": round(execution_time * 1000, 2),
                },
                "recommendations": [
                    "Unable to analyze query - no execution plan returned"
                ],
                "metadata": {
                    "query_analyzed": query[:100] + ("..." if len(query) > 100 else ""),
                    "analysis_failed": True,
                },
            }

        logger.info(f"Query performance analysis completed in {execution_time:.3f}s")

        return format_success_response(
            data=performance_analysis,
            message="Query performance analysis completed successfully",
        )

    except (ValidationError, SecurityError) as e:
        logger.warning(f"Query performance analysis validation/security error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Query performance analysis error: {e}")
        mcp_error = handle_postgres_error(e, query, parameters)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def find_slow_queries(
    min_duration_ms: float = 1000.0, limit: int = 10
) -> dict[str, Any]:
    """Find slow queries from PostgreSQL statistics.

    This tool queries pg_stat_statements to identify poorly performing queries
    based on execution time and frequency.

    Args:
        min_duration_ms: Minimum average execution time in milliseconds
        limit: Maximum number of slow queries to return

    Returns:
        Dictionary containing slow query statistics and analysis

    Raises:
        ValidationError: If parameters are invalid
        QueryExecutionError: If query execution fails
    """
    try:
        # Validate inputs
        if min_duration_ms < 0:
            raise ValidationError("min_duration_ms must be non-negative")

        if limit < 1 or limit > 100:
            raise ValidationError("limit must be between 1 and 100")

        # Record start time
        start_time = time.time()

        # First check if pg_stat_statements extension is available
        check_extension_query = """
        SELECT EXISTS (
            SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
        ) as extension_exists
        """

        extension_exists = await connection_manager.execute_query(
            query=check_extension_query, fetch_mode="val"
        )

        if not extension_exists:
            # Fallback to current session queries from pg_stat_activity
            fallback_query = """
            SELECT
                pid,
                usename as username,
                datname as database,
                state,
                query_start,
                EXTRACT(EPOCH FROM (now() - query_start)) * 1000 as duration_ms,
                LEFT(query, 100) as query_preview
            FROM pg_stat_activity
            WHERE state = 'active'
                AND query != '<IDLE>'
                AND query NOT LIKE '%pg_stat_activity%'
                AND EXTRACT(EPOCH FROM (now() - query_start)) * 1000 >= $1
            ORDER BY duration_ms DESC
            LIMIT $2
            """

            result = await connection_manager.execute_query(
                query=fallback_query,
                parameters=[min_duration_ms, limit],
                fetch_mode="all",
            )

            slow_queries = []
            for row in result:
                row_dict = dict(row)
                slow_queries.append(
                    {
                        "query_preview": row_dict.get("query_preview", ""),
                        "duration_ms": row_dict.get("duration_ms", 0),
                        "username": row_dict.get("username", ""),
                        "database": row_dict.get("database", ""),
                        "state": row_dict.get("state", ""),
                        "source": "pg_stat_activity",
                    }
                )

            analysis_result = {
                "slow_queries": slow_queries,
                "total_found": len(slow_queries),
                "analysis_method": "pg_stat_activity",
                "warning": "pg_stat_statements extension not available - showing only currently active queries",
                "metadata": {
                    "min_duration_ms": min_duration_ms,
                    "limit": limit,
                    "analysis_time_ms": round((time.time() - start_time) * 1000, 2),
                },
            }

        else:
            # Use pg_stat_statements for comprehensive analysis
            slow_queries_query = """
            SELECT
                LEFT(query, 100) as query_preview,
                calls,
                total_exec_time,
                mean_exec_time,
                max_exec_time,
                min_exec_time,
                stddev_exec_time,
                rows as total_rows,
                (total_exec_time / calls) as avg_time_per_call,
                (100.0 * total_exec_time / sum(total_exec_time) OVER()) as percent_total_time
            FROM pg_stat_statements
            WHERE mean_exec_time >= $1
            ORDER BY mean_exec_time DESC
            LIMIT $2
            """

            result = await connection_manager.execute_query(
                query=slow_queries_query,
                parameters=[min_duration_ms, limit],
                fetch_mode="all",
            )

            slow_queries = []
            for row in result:
                row_dict = dict(row)
                slow_queries.append(
                    {
                        "query_preview": row_dict.get("query_preview", ""),
                        "calls": row_dict.get("calls", 0),
                        "total_exec_time_ms": row_dict.get("total_exec_time", 0),
                        "mean_exec_time_ms": row_dict.get("mean_exec_time", 0),
                        "max_exec_time_ms": row_dict.get("max_exec_time", 0),
                        "min_exec_time_ms": row_dict.get("min_exec_time", 0),
                        "stddev_exec_time_ms": row_dict.get("stddev_exec_time", 0),
                        "total_rows": row_dict.get("total_rows", 0),
                        "avg_time_per_call_ms": row_dict.get("avg_time_per_call", 0),
                        "percent_total_time": round(
                            row_dict.get("percent_total_time", 0), 2
                        ),
                        "source": "pg_stat_statements",
                    }
                )

            analysis_result = {
                "slow_queries": slow_queries,
                "total_found": len(slow_queries),
                "analysis_method": "pg_stat_statements",
                "recommendations": _generate_slow_query_recommendations(slow_queries),
                "metadata": {
                    "min_duration_ms": min_duration_ms,
                    "limit": limit,
                    "analysis_time_ms": round((time.time() - start_time) * 1000, 2),
                },
            }

        execution_time = time.time() - start_time
        logger.info(f"Slow query analysis completed in {execution_time:.3f}s")

        return format_success_response(
            data=analysis_result, message="Slow query analysis completed successfully"
        )

    except ValidationError as e:
        logger.warning(f"Slow query analysis validation error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Slow query analysis error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


async def get_table_stats(table_name: str) -> dict[str, Any]:
    """Get comprehensive table statistics including storage and access patterns.

    This tool provides detailed statistics about table size, index usage,
    and access patterns to help with performance optimization.

    Args:
        table_name: Name of the table to analyze

    Returns:
        Dictionary containing table statistics and storage information

    Raises:
        ValidationError: If table_name is invalid
        QueryExecutionError: If statistics query fails
    """
    try:
        # Validate inputs
        if not table_name or not table_name.strip():
            raise ValidationError("Table name cannot be empty")

        # Sanitize table name (basic validation)
        clean_table_name = table_name.strip()
        if not clean_table_name.replace("_", "").replace(".", "").isalnum():
            raise ValidationError("Table name contains invalid characters")

        # Record start time
        start_time = time.time()

        # Check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_name = $1 AND table_schema = 'public'
        ) as table_exists
        """

        table_exists = await connection_manager.execute_query(
            query=table_exists_query, parameters=[clean_table_name], fetch_mode="val"
        )

        if not table_exists:
            raise ValidationError(f"Table '{clean_table_name}' does not exist")

        # Get basic table statistics
        basic_stats_query = """
        SELECT
            schemaname,
            tablename,
            attname as column_name,
            n_distinct,
            most_common_vals,
            most_common_freqs,
            histogram_bounds,
            correlation
        FROM pg_stats
        WHERE tablename = $1 AND schemaname = 'public'
        ORDER BY attname
        """

        basic_stats = await connection_manager.execute_query(
            query=basic_stats_query, parameters=[clean_table_name], fetch_mode="all"
        )

        # Get table size information
        size_query = """
        SELECT
            pg_size_pretty(pg_total_relation_size($1)) as total_size,
            pg_size_pretty(pg_relation_size($1)) as table_size,
            pg_size_pretty(pg_total_relation_size($1) - pg_relation_size($1)) as index_size,
            pg_total_relation_size($1) as total_size_bytes,
            pg_relation_size($1) as table_size_bytes
        """

        size_info = await connection_manager.execute_query(
            query=size_query, parameters=[clean_table_name], fetch_mode="one"
        )

        # Get row count and table statistics
        table_stats_query = """
        SELECT
            n_tup_ins as inserts,
            n_tup_upd as updates,
            n_tup_del as deletes,
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
        WHERE relname = $1
        """

        table_stats = await connection_manager.execute_query(
            query=table_stats_query, parameters=[clean_table_name], fetch_mode="one"
        )

        # Get index usage statistics
        index_stats_query = """
        SELECT
            indexrelname as index_name,
            idx_tup_read as index_tuples_read,
            idx_tup_fetch as index_tuples_fetched,
            idx_scan as index_scans,
            pg_size_pretty(pg_relation_size(indexrelid)) as index_size
        FROM pg_stat_user_indexes
        WHERE relname = $1
        ORDER BY idx_scan DESC
        """

        index_stats = await connection_manager.execute_query(
            query=index_stats_query, parameters=[clean_table_name], fetch_mode="all"
        )

        # Format column statistics
        column_stats = []
        for row in basic_stats:
            row_dict = dict(row)
            column_stats.append(
                {
                    "column_name": row_dict.get("column_name"),
                    "n_distinct": row_dict.get("n_distinct"),
                    "most_common_values": row_dict.get("most_common_vals"),
                    "most_common_frequencies": row_dict.get("most_common_freqs"),
                    "correlation": row_dict.get("correlation"),
                }
            )

        # Format index statistics
        index_usage = []
        for row in index_stats:
            row_dict = dict(row)
            index_usage.append(
                {
                    "index_name": row_dict.get("index_name"),
                    "scans": row_dict.get("index_scans", 0),
                    "tuples_read": row_dict.get("index_tuples_read", 0),
                    "tuples_fetched": row_dict.get("index_tuples_fetched", 0),
                    "size": row_dict.get("index_size"),
                }
            )

        # Compile comprehensive statistics
        size_dict = dict(size_info) if size_info else {}
        stats_dict = dict(table_stats) if table_stats else {}

        table_statistics = {
            "table_name": clean_table_name,
            "size_information": {
                "total_size": size_dict.get("total_size"),
                "table_size": size_dict.get("table_size"),
                "index_size": size_dict.get("index_size"),
                "total_size_bytes": size_dict.get("total_size_bytes", 0),
                "table_size_bytes": size_dict.get("table_size_bytes", 0),
            },
            "row_statistics": {
                "live_tuples": stats_dict.get("live_tuples", 0),
                "dead_tuples": stats_dict.get("dead_tuples", 0),
                "total_inserts": stats_dict.get("inserts", 0),
                "total_updates": stats_dict.get("updates", 0),
                "total_deletes": stats_dict.get("deletes", 0),
            },
            "maintenance_statistics": {
                "last_vacuum": serialize_value(stats_dict.get("last_vacuum")),
                "last_autovacuum": serialize_value(stats_dict.get("last_autovacuum")),
                "last_analyze": serialize_value(stats_dict.get("last_analyze")),
                "last_autoanalyze": serialize_value(stats_dict.get("last_autoanalyze")),
                "vacuum_count": stats_dict.get("vacuum_count", 0),
                "autovacuum_count": stats_dict.get("autovacuum_count", 0),
                "analyze_count": stats_dict.get("analyze_count", 0),
                "autoanalyze_count": stats_dict.get("autoanalyze_count", 0),
            },
            "column_statistics": column_stats,
            "index_usage": index_usage,
            "recommendations": _generate_table_recommendations(
                stats_dict, size_dict, index_usage
            ),
            "metadata": {
                "analysis_time_ms": round((time.time() - start_time) * 1000, 2),
                "column_count": len(column_stats),
                "index_count": len(index_usage),
            },
        }

        execution_time = time.time() - start_time
        logger.info(f"Table statistics analysis completed in {execution_time:.3f}s")

        return format_success_response(
            data=table_statistics,
            message="Table statistics analysis completed successfully",
        )

    except ValidationError as e:
        logger.warning(f"Table statistics validation error: {e}")
        return format_error_response(e.error_code, str(e), e.details)

    except Exception as e:
        logger.error(f"Table statistics error: {e}")
        mcp_error = handle_postgres_error(e)
        return format_error_response(
            mcp_error.error_code, str(mcp_error), mcp_error.details
        )


def _generate_performance_recommendations(plan: dict[str, Any]) -> list[str]:
    """Generate performance recommendations based on query execution plan."""
    recommendations = []

    if not plan:
        return ["Unable to generate recommendations - no execution plan available"]

    # Check for sequential scans
    node_type = plan.get("Node Type", "")
    if node_type == "Seq Scan":
        recommendations.append(
            "Consider adding an index - query is using sequential scan"
        )

    # Check for high cost operations
    total_cost = plan.get("Total Cost", 0)
    if total_cost > 1000:
        recommendations.append(
            f"High query cost ({total_cost:.2f}) - consider query optimization"
        )

    # Check for row estimate accuracy
    actual_rows = plan.get("Actual Rows", 0)
    planned_rows = plan.get("Plan Rows", 0)
    if actual_rows > 0 and planned_rows > 0:
        ratio = actual_rows / planned_rows
        if ratio > 10 or ratio < 0.1:
            recommendations.append(
                "Row estimates are inaccurate - consider running ANALYZE on tables"
            )

    # Check for nested loops with high row counts
    if node_type == "Nested Loop" and actual_rows > 1000:
        recommendations.append(
            "Nested loop with high row count - consider hash join or merge join"
        )

    if not recommendations:
        recommendations.append("Query performance appears optimal")

    return recommendations


def _generate_slow_query_recommendations(
    slow_queries: list[dict[str, Any]],
) -> list[str]:
    """Generate recommendations based on slow query analysis."""
    recommendations = []

    if not slow_queries:
        return ["No slow queries found - database performance appears good"]

    # Analyze patterns in slow queries
    high_call_count = any(q.get("calls", 0) > 1000 for q in slow_queries)
    high_variance = any(
        q.get("stddev_exec_time_ms", 0) > q.get("mean_exec_time_ms", 0) * 0.5
        for q in slow_queries
    )

    if high_call_count:
        recommendations.append(
            "Some queries have high call frequency - consider caching or optimization"
        )

    if high_variance:
        recommendations.append(
            "High execution time variance detected - investigate query plan stability"
        )

    recommendations.extend(
        [
            "Review and optimize the slowest queries first",
            "Consider adding appropriate indexes for frequently used queries",
            "Analyze query patterns to identify optimization opportunities",
        ]
    )

    return recommendations


def _generate_table_recommendations(
    stats: dict[str, Any], size: dict[str, Any], indexes: list[dict[str, Any]]
) -> list[str]:
    """Generate recommendations based on table statistics."""
    recommendations = []

    # Check dead tuple ratio
    live_tuples = stats.get("live_tuples", 0)
    dead_tuples = stats.get("dead_tuples", 0)
    if live_tuples > 0:
        dead_ratio = dead_tuples / (live_tuples + dead_tuples)
        if dead_ratio > 0.1:
            recommendations.append(
                f"High dead tuple ratio ({dead_ratio:.1%}) - consider running VACUUM"
            )

    # Check table size
    total_size_bytes = size.get("total_size_bytes", 0)
    if total_size_bytes > 1024 * 1024 * 1024:  # > 1GB
        recommendations.append(
            "Large table detected - monitor performance and consider partitioning"
        )

    # Check index usage
    unused_indexes = [idx for idx in indexes if idx.get("scans", 0) == 0]
    if unused_indexes:
        recommendations.append(
            f"Found {len(unused_indexes)} unused indexes - consider dropping them"
        )

    # Check maintenance
    if not stats.get("last_analyze"):
        recommendations.append("Table has never been analyzed - run ANALYZE")

    if not recommendations:
        recommendations.append("Table statistics appear healthy")

    return recommendations


# Tool schema definitions for MCP registration
ANALYZE_QUERY_PERFORMANCE_SCHEMA = {
    "name": "analyze_query_performance",
    "description": "Analyze query performance and provide execution plan analysis with EXPLAIN ANALYZE",
    "inputSchema": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query to analyze for performance",
            },
            "parameters": {
                "type": "array",
                "description": "List of parameters to bind to the query placeholders",
                "items": {"type": ["string", "number", "boolean", "null"]},
                "default": [],
            },
        },
        "required": ["query"],
    },
}

FIND_SLOW_QUERIES_SCHEMA = {
    "name": "find_slow_queries",
    "description": "Find slow queries from PostgreSQL statistics to identify performance bottlenecks",
    "inputSchema": {
        "type": "object",
        "properties": {
            "min_duration_ms": {
                "type": "number",
                "description": "Minimum average execution time in milliseconds to consider a query slow",
                "default": 1000.0,
                "minimum": 0,
            },
            "limit": {
                "type": "integer",
                "description": "Maximum number of slow queries to return",
                "default": 10,
                "minimum": 1,
                "maximum": 100,
            },
        },
        "required": [],
    },
}

GET_TABLE_STATS_SCHEMA = {
    "name": "get_table_stats",
    "description": "Get comprehensive table statistics including storage usage and access patterns",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to analyze",
            },
        },
        "required": ["table_name"],
    },
}

# Export tool functions and schemas
__all__ = [
    "analyze_query_performance",
    "find_slow_queries",
    "get_table_stats",
    "ANALYZE_QUERY_PERFORMANCE_SCHEMA",
    "FIND_SLOW_QUERIES_SCHEMA",
    "GET_TABLE_STATS_SCHEMA",
]
