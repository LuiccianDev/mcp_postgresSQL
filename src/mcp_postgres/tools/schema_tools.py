"""Schema inspection tools for MCP Postgres server.

This module provides tools for inspecting database schema including tables,
indexes, constraints, views, functions, triggers, and sequences.
"""

import logging
from typing import Any

from ..core.connection import connection_manager
from ..utils.exceptions import (
    MCPPostgresError,
    TableNotFoundError,
    handle_postgres_error,
)
from ..utils.formatters import (
    format_table_info,
    format_table_list,
)
from ..utils.validators import validate_table_name


logger = logging.getLogger(__name__)


async def list_tables(schema_name: str | None = None) -> dict[str, Any]:
    """List all tables in the database with metadata.

    Args:
        schema_name: Optional schema name to filter tables (defaults to 'public')

    Returns:
        Dictionary containing list of tables with metadata

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        # Default to public schema if not specified
        if schema_name is None:
            schema_name = "public"

        # Validate schema name using same rules as table names
        validate_table_name(schema_name)

        query = """
        SELECT
            t.table_name,
            t.table_type,
            t.table_schema,
            pg_size_pretty(pg_total_relation_size(c.oid)) as size_human,
            pg_total_relation_size(c.oid) as size_bytes,
            obj_description(c.oid, 'pg_class') as comment,
            c.reltuples::bigint as estimated_rows
        FROM information_schema.tables t
        LEFT JOIN pg_class c ON c.relname = t.table_name
        LEFT JOIN pg_namespace n ON n.nspname = t.table_schema AND c.relnamespace = n.oid
        WHERE t.table_schema = $1
        AND t.table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY t.table_name
        """

        rows = await connection_manager.execute_query(query, [schema_name])

        # Convert asyncpg Records to dictionaries
        tables = []
        for row in rows:
            table_dict = dict(row)
            tables.append(table_dict)

        logger.info(f"Retrieved {len(tables)} tables from schema '{schema_name}'")
        return format_table_list(tables)

    except Exception as e:
        logger.error(f"Error listing tables: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def describe_table(
    table_name: str, schema_name: str | None = None
) -> dict[str, Any]:
    """Get detailed information about a specific table structure.

    Args:
        table_name: Name of the table to describe
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing detailed table structure information

    Raises:
        MCPPostgresError: If table doesn't exist or query fails
    """
    try:
        validate_table_name(table_name)

        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        # First check if table exists
        table_exists_query = """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        """

        exists = await connection_manager.execute_query(
            table_exists_query, [schema_name, table_name], "val"
        )

        if not exists:
            raise TableNotFoundError(table_name, schema_name)

        # Get detailed column information
        columns_query = """
        SELECT
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key,
            CASE WHEN fk.column_name IS NOT NULL THEN true ELSE false END as is_foreign_key,
            fk.foreign_table_name,
            fk.foreign_column_name,
            col_description(pgc.oid, c.ordinal_position) as comment
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT ku.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
                ON tc.constraint_name = ku.constraint_name
                AND tc.table_schema = ku.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) pk ON pk.column_name = c.column_name
        LEFT JOIN (
            SELECT
                ku.column_name,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage ku
                ON tc.constraint_name = ku.constraint_name
                AND tc.table_schema = ku.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) fk ON fk.column_name = c.column_name
        LEFT JOIN pg_class pgc ON pgc.relname = c.table_name
        LEFT JOIN pg_namespace pgn ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        """

        column_rows = await connection_manager.execute_query(
            columns_query, [schema_name, table_name]
        )

        columns = [dict(row) for row in column_rows]

        logger.info(
            f"Retrieved {len(columns)} columns for table '{schema_name}.{table_name}'"
        )
        return format_table_info(table_name, columns)

    except Exception as e:
        logger.error(f"Error describing table '{table_name}': {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_indexes(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """List indexes with performance and usage information.

    Args:
        table_name: Optional table name to filter indexes
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing index information and statistics

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

        # Build query based on whether table_name is specified
        if table_name:
            query = """
            SELECT
                i.indexname as index_name,
                i.tablename as table_name,
                i.schemaname as schema_name,
                i.indexdef as index_definition,
                pg_size_pretty(pg_relation_size(c.oid)) as size_human,
                pg_relation_size(c.oid) as size_bytes,
                CASE WHEN i.indexname ~ '_pkey$' THEN 'PRIMARY KEY'
                     WHEN i.indexname ~ '_key$' THEN 'UNIQUE'
                     ELSE 'INDEX' END as index_type,
                s.idx_scan as scans,
                s.idx_tup_read as tuples_read,
                s.idx_tup_fetch as tuples_fetched
            FROM pg_indexes i
            LEFT JOIN pg_class c ON c.relname = i.indexname
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelname = i.indexname
            WHERE i.schemaname = $1 AND i.tablename = $2
            ORDER BY i.indexname
            """
            parameters = [schema_name, table_name]
        else:
            query = """
            SELECT
                i.indexname as index_name,
                i.tablename as table_name,
                i.schemaname as schema_name,
                i.indexdef as index_definition,
                pg_size_pretty(pg_relation_size(c.oid)) as size_human,
                pg_relation_size(c.oid) as size_bytes,
                CASE WHEN i.indexname ~ '_pkey$' THEN 'PRIMARY KEY'
                     WHEN i.indexname ~ '_key$' THEN 'UNIQUE'
                     ELSE 'INDEX' END as index_type,
                s.idx_scan as scans,
                s.idx_tup_read as tuples_read,
                s.idx_tup_fetch as tuples_fetched
            FROM pg_indexes i
            LEFT JOIN pg_class c ON c.relname = i.indexname
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelname = i.indexname
            WHERE i.schemaname = $1
            ORDER BY i.tablename, i.indexname
            """
            parameters = [schema_name]

        rows = await connection_manager.execute_query(query, parameters)

        indexes = [dict(row) for row in rows]

        # Calculate summary statistics
        total_size = sum(idx.get("size_bytes", 0) or 0 for idx in indexes)
        total_scans = sum(idx.get("scans", 0) or 0 for idx in indexes)

        result = {
            "indexes": indexes,
            "index_count": len(indexes),
            "total_size_bytes": total_size,
            "total_size_human": f"{total_size / (1024**2):.1f} MB"
            if total_size > 0
            else "0 B",
            "total_scans": total_scans,
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "has_usage_stats": any(idx.get("scans") is not None for idx in indexes),
            },
        }

        logger.info(
            f"Retrieved {len(indexes)} indexes"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" from schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error listing indexes: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_constraints(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """List table constraints including foreign keys, check constraints, and unique constraints.

    Args:
        table_name: Optional table name to filter constraints
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing constraint information

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

        # Build query based on whether table_name is specified
        if table_name:
            query = """
            SELECT
                tc.constraint_name,
                tc.table_name,
                tc.table_schema,
                tc.constraint_type,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name,
                rc.update_rule,
                rc.delete_rule,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            LEFT JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.table_schema = cc.constraint_schema
            WHERE tc.table_schema = $1 AND tc.table_name = $2
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema, tc.constraint_type,
                     ccu.table_name, ccu.column_name, rc.update_rule, rc.delete_rule, cc.check_clause
            ORDER BY tc.constraint_type, tc.constraint_name
            """
            parameters = [schema_name, table_name]
        else:
            query = """
            SELECT
                tc.constraint_name,
                tc.table_name,
                tc.table_schema,
                tc.constraint_type,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns,
                ccu.table_name as foreign_table_name,
                ccu.column_name as foreign_column_name,
                rc.update_rule,
                rc.delete_rule,
                cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            LEFT JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.constraint_schema
            LEFT JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
                AND tc.table_schema = rc.constraint_schema
            LEFT JOIN information_schema.check_constraints cc
                ON tc.constraint_name = cc.constraint_name
                AND tc.table_schema = cc.constraint_schema
            WHERE tc.table_schema = $1
            GROUP BY tc.constraint_name, tc.table_name, tc.table_schema, tc.constraint_type,
                     ccu.table_name, ccu.column_name, rc.update_rule, rc.delete_rule, cc.check_clause
            ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name
            """
            parameters = [schema_name]

        rows = await connection_manager.execute_query(query, parameters)

        constraints = [dict(row) for row in rows]

        # Group constraints by type for summary
        constraint_types = {}
        for constraint in constraints:
            constraint_type = constraint["constraint_type"]
            if constraint_type not in constraint_types:
                constraint_types[constraint_type] = 0
            constraint_types[constraint_type] += 1

        result = {
            "constraints": constraints,
            "constraint_count": len(constraints),
            "constraint_types": constraint_types,
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "has_foreign_keys": any(
                    c["constraint_type"] == "FOREIGN KEY" for c in constraints
                ),
                "has_check_constraints": any(
                    c["constraint_type"] == "CHECK" for c in constraints
                ),
            },
        }

        logger.info(
            f"Retrieved {len(constraints)} constraints"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" from schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error listing constraints: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_views(schema_name: str | None = None) -> dict[str, Any]:
    """List database views with their definitions and dependencies.

    Args:
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing view information and definitions

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        query = """
        SELECT
            v.table_name as view_name,
            v.table_schema as schema_name,
            v.view_definition,
            v.check_option,
            v.is_updatable,
            v.is_insertable_into,
            v.is_trigger_updatable,
            v.is_trigger_deletable,
            v.is_trigger_insertable_into,
            obj_description(c.oid, 'pg_class') as comment
        FROM information_schema.views v
        LEFT JOIN pg_class c ON c.relname = v.table_name
        LEFT JOIN pg_namespace n ON n.nspname = v.table_schema AND c.relnamespace = n.oid
        WHERE v.table_schema = $1
        ORDER BY v.table_name
        """

        rows = await connection_manager.execute_query(query, [schema_name])

        views = [dict(row) for row in rows]

        # Get view dependencies (tables/views that this view depends on)
        for view in views:
            view_name = view["view_name"]

            dependencies_query = """
            SELECT DISTINCT
                ref_nsp.nspname as referenced_schema,
                ref_class.relname as referenced_table,
                ref_class.relkind as referenced_type
            FROM pg_depend d
            JOIN pg_rewrite r ON r.oid = d.objid
            JOIN pg_class dependent_view ON dependent_view.oid = r.ev_class
            JOIN pg_class ref_class ON ref_class.oid = d.refobjid
            JOIN pg_namespace ref_nsp ON ref_nsp.oid = ref_class.relnamespace
            JOIN pg_namespace dependent_nsp ON dependent_nsp.oid = dependent_view.relnamespace
            WHERE dependent_view.relname = $1
            AND dependent_nsp.nspname = $2
            AND ref_class.relkind IN ('r', 'v', 'm')  -- tables, views, materialized views
            AND d.deptype = 'n'  -- normal dependency
            ORDER BY ref_nsp.nspname, ref_class.relname
            """

            try:
                dep_rows = await connection_manager.execute_query(
                    dependencies_query, [view_name, schema_name]
                )
                view["dependencies"] = [dict(row) for row in dep_rows]
            except Exception as dep_error:
                logger.warning(
                    f"Could not get dependencies for view '{view_name}': {dep_error}"
                )
                view["dependencies"] = []

        result = {
            "views": views,
            "view_count": len(views),
            "metadata": {
                "schema_name": schema_name,
                "updatable_views": sum(
                    1 for v in views if v.get("is_updatable") == "YES"
                ),
                "insertable_views": sum(
                    1 for v in views if v.get("is_insertable_into") == "YES"
                ),
                "has_dependencies": any(v.get("dependencies") for v in views),
            },
        }

        logger.info(f"Retrieved {len(views)} views from schema '{schema_name}'")
        return result

    except Exception as e:
        logger.error(f"Error listing views: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_functions(schema_name: str | None = None) -> dict[str, Any]:
    """List stored procedures and functions with their signatures and properties.

    Args:
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing function information

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        query = """
        SELECT
            r.routine_name as function_name,
            r.routine_schema as schema_name,
            r.routine_type,
            r.data_type as return_type,
            r.routine_definition as function_definition,
            r.external_language as language,
            r.is_deterministic,
            r.sql_data_access,
            r.is_null_call,
            r.routine_comment as comment,
            string_agg(
                p.parameter_name || ' ' || p.data_type ||
                CASE WHEN p.parameter_mode != 'IN' THEN ' (' || p.parameter_mode || ')' ELSE '' END,
                ', ' ORDER BY p.ordinal_position
            ) as parameters
        FROM information_schema.routines r
        LEFT JOIN information_schema.parameters p
            ON r.routine_name = p.specific_name
            AND r.routine_schema = p.specific_schema
        WHERE r.routine_schema = $1
        AND r.routine_type IN ('FUNCTION', 'PROCEDURE')
        GROUP BY r.routine_name, r.routine_schema, r.routine_type, r.data_type,
                 r.routine_definition, r.external_language, r.is_deterministic,
                 r.sql_data_access, r.is_null_call, r.routine_comment
        ORDER BY r.routine_type, r.routine_name
        """

        rows = await connection_manager.execute_query(query, [schema_name])

        functions = [dict(row) for row in rows]

        # Group functions by type for summary
        function_types = {}
        languages = {}

        for func in functions:
            func_type = func["routine_type"]
            language = func["language"]

            if func_type not in function_types:
                function_types[func_type] = 0
            function_types[func_type] += 1

            if language not in languages:
                languages[language] = 0
            languages[language] += 1

        result = {
            "functions": functions,
            "function_count": len(functions),
            "function_types": function_types,
            "languages": languages,
            "metadata": {
                "schema_name": schema_name,
                "has_procedures": "PROCEDURE" in function_types,
                "has_sql_functions": "sql" in languages,
                "has_plpgsql_functions": "plpgsql" in languages,
            },
        }

        logger.info(
            f"Retrieved {len(functions)} functions/procedures from schema '{schema_name}'"
        )
        return result

    except Exception as e:
        logger.error(f"Error listing functions: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_triggers(
    table_name: str | None = None, schema_name: str | None = None
) -> dict[str, Any]:
    """List database triggers with their definitions and properties.

    Args:
        table_name: Optional table name to filter triggers
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing trigger information

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        if table_name is not None:
            validate_table_name(table_name)

        # Build query based on whether table_name is specified
        if table_name:
            query = """
            SELECT
                t.trigger_name,
                t.event_object_table as table_name,
                t.event_object_schema as schema_name,
                t.trigger_schema,
                t.event_manipulation as trigger_event,
                t.action_timing,
                t.action_orientation,
                t.action_statement,
                t.action_condition,
                pg_get_triggerdef(pg_trigger.oid) as trigger_definition
            FROM information_schema.triggers t
            LEFT JOIN pg_trigger ON pg_trigger.tgname = t.trigger_name
            LEFT JOIN pg_class ON pg_class.oid = pg_trigger.tgrelid
            LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE t.event_object_schema = $1 AND t.event_object_table = $2
            ORDER BY t.trigger_name
            """
            parameters = [schema_name, table_name]
        else:
            query = """
            SELECT
                t.trigger_name,
                t.event_object_table as table_name,
                t.event_object_schema as schema_name,
                t.trigger_schema,
                t.event_manipulation as trigger_event,
                t.action_timing,
                t.action_orientation,
                t.action_statement,
                t.action_condition,
                pg_get_triggerdef(pg_trigger.oid) as trigger_definition
            FROM information_schema.triggers t
            LEFT JOIN pg_trigger ON pg_trigger.tgname = t.trigger_name
            LEFT JOIN pg_class ON pg_class.oid = pg_trigger.tgrelid
            LEFT JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
            WHERE t.event_object_schema = $1
            ORDER BY t.event_object_table, t.trigger_name
            """
            parameters = [schema_name]

        rows = await connection_manager.execute_query(query, parameters)

        triggers = [dict(row) for row in rows]

        # Group triggers by event type and timing for summary
        event_types = {}
        timing_types = {}

        for trigger in triggers:
            event = trigger["trigger_event"]
            timing = trigger["action_timing"]

            if event not in event_types:
                event_types[event] = 0
            event_types[event] += 1

            if timing not in timing_types:
                timing_types[timing] = 0
            timing_types[timing] += 1

        result = {
            "triggers": triggers,
            "trigger_count": len(triggers),
            "event_types": event_types,
            "timing_types": timing_types,
            "metadata": {
                "schema_name": schema_name,
                "table_name": table_name,
                "has_before_triggers": "BEFORE" in timing_types,
                "has_after_triggers": "AFTER" in timing_types,
                "has_instead_of_triggers": "INSTEAD OF" in timing_types,
            },
        }

        logger.info(
            f"Retrieved {len(triggers)} triggers"
            + (
                f" for table '{table_name}'"
                if table_name
                else f" from schema '{schema_name}'"
            )
        )
        return result

    except Exception as e:
        logger.error(f"Error listing triggers: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


async def list_sequences(schema_name: str | None = None) -> dict[str, Any]:
    """List database sequences with their current values and properties.

    Args:
        schema_name: Optional schema name (defaults to 'public')

    Returns:
        Dictionary containing sequence information

    Raises:
        MCPPostgresError: If query execution fails
    """
    try:
        if schema_name is None:
            schema_name = "public"
        else:
            validate_table_name(schema_name)

        query = """
        SELECT
            s.sequence_name,
            s.sequence_schema as schema_name,
            s.data_type,
            s.numeric_precision,
            s.numeric_scale,
            s.start_value,
            s.minimum_value,
            s.maximum_value,
            s.increment,
            s.cycle_option,
            pg_sequence_last_value(c.oid) as last_value,
            CASE
                WHEN pg_sequence_last_value(c.oid) IS NOT NULL
                THEN (s.maximum_value - pg_sequence_last_value(c.oid)) / s.increment
                ELSE NULL
            END as remaining_values,
            obj_description(c.oid, 'pg_class') as comment
        FROM information_schema.sequences s
        LEFT JOIN pg_class c ON c.relname = s.sequence_name
        LEFT JOIN pg_namespace n ON n.nspname = s.sequence_schema AND c.relnamespace = n.oid
        WHERE s.sequence_schema = $1
        ORDER BY s.sequence_name
        """

        rows = await connection_manager.execute_query(query, [schema_name])

        sequences = [dict(row) for row in rows]

        # Get sequence ownership information (which columns use these sequences)
        for sequence in sequences:
            seq_name = sequence["sequence_name"]

            ownership_query = """
            SELECT
                t.table_name,
                c.column_name,
                t.table_schema
            FROM pg_depend d
            JOIN pg_class seq ON seq.oid = d.objid
            JOIN pg_class t ON t.oid = d.refobjid
            JOIN pg_attribute c ON c.attrelid = t.oid AND c.attnum = d.refobjsubid
            JOIN pg_namespace ns ON ns.oid = seq.relnamespace
            JOIN pg_namespace nt ON nt.oid = t.relnamespace
            WHERE seq.relname = $1
            AND ns.nspname = $2
            AND seq.relkind = 'S'
            AND d.deptype = 'a'  -- auto dependency
            """

            try:
                owner_rows = await connection_manager.execute_query(
                    ownership_query, [seq_name, schema_name]
                )
                sequence["owned_by"] = [dict(row) for row in owner_rows]
            except Exception as owner_error:
                logger.warning(
                    f"Could not get ownership info for sequence '{seq_name}': {owner_error}"
                )
                sequence["owned_by"] = []

        # Calculate summary statistics
        total_sequences = len(sequences)
        cycling_sequences = sum(1 for s in sequences if s.get("cycle_option") == "YES")
        sequences_with_values = sum(
            1 for s in sequences if s.get("last_value") is not None
        )

        result = {
            "sequences": sequences,
            "sequence_count": total_sequences,
            "cycling_sequences": cycling_sequences,
            "sequences_with_values": sequences_with_values,
            "metadata": {
                "schema_name": schema_name,
                "has_cycling_sequences": cycling_sequences > 0,
                "has_owned_sequences": any(s.get("owned_by") for s in sequences),
            },
        }

        logger.info(f"Retrieved {len(sequences)} sequences from schema '{schema_name}'")
        return result

    except Exception as e:
        logger.error(f"Error listing sequences: {e}")
        if isinstance(e, MCPPostgresError):
            raise
        raise handle_postgres_error(e) from e


# Tool schema definitions for MCP registration
LIST_TABLES_SCHEMA = {
    "name": "list_tables",
    "description": "List all tables in the database with metadata including size, type, and row estimates",
    "inputSchema": {
        "type": "object",
        "properties": {
            "schema_name": {
                "type": "string",
                "description": "Schema name to filter tables (defaults to 'public')",
                "default": "public",
            }
        },
        "required": [],
    },
}

DESCRIBE_TABLE_SCHEMA = {
    "name": "describe_table",
    "description": "Get detailed information about a specific table structure including columns, types, and constraints",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Name of the table to describe",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
        },
        "required": ["table_name"],
    },
}

LIST_INDEXES_SCHEMA = {
    "name": "list_indexes",
    "description": "List indexes with performance and usage information including size and scan statistics",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to filter indexes",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
        },
        "required": [],
    },
}

LIST_CONSTRAINTS_SCHEMA = {
    "name": "list_constraints",
    "description": "List table constraints including foreign keys, check constraints, and unique constraints",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to filter constraints",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
        },
        "required": [],
    },
}

LIST_VIEWS_SCHEMA = {
    "name": "list_views",
    "description": "List database views with their definitions and dependencies",
    "inputSchema": {
        "type": "object",
        "properties": {
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            }
        },
        "required": [],
    },
}

LIST_FUNCTIONS_SCHEMA = {
    "name": "list_functions",
    "description": "List stored procedures and functions with their signatures and properties",
    "inputSchema": {
        "type": "object",
        "properties": {
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            }
        },
        "required": [],
    },
}

LIST_TRIGGERS_SCHEMA = {
    "name": "list_triggers",
    "description": "List database triggers with their definitions and properties",
    "inputSchema": {
        "type": "object",
        "properties": {
            "table_name": {
                "type": "string",
                "description": "Optional table name to filter triggers",
            },
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            },
        },
        "required": [],
    },
}

LIST_SEQUENCES_SCHEMA = {
    "name": "list_sequences",
    "description": "List database sequences with their current values and properties",
    "inputSchema": {
        "type": "object",
        "properties": {
            "schema_name": {
                "type": "string",
                "description": "Schema name (defaults to 'public')",
                "default": "public",
            }
        },
        "required": [],
    },
}

# Export tool functions and schemas
__all__ = [
    "list_tables",
    "describe_table",
    "list_indexes",
    "list_constraints",
    "list_views",
    "list_functions",
    "list_triggers",
    "list_sequences",
    "LIST_TABLES_SCHEMA",
    "DESCRIBE_TABLE_SCHEMA",
    "LIST_INDEXES_SCHEMA",
    "LIST_CONSTRAINTS_SCHEMA",
    "LIST_VIEWS_SCHEMA",
    "LIST_FUNCTIONS_SCHEMA",
    "LIST_TRIGGERS_SCHEMA",
    "LIST_SEQUENCES_SCHEMA",
]
