"""Unit tests for schema tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.schema_tools import (
    describe_table,
    list_constraints,
    list_functions,
    list_indexes,
    list_sequences,
    list_tables,
    list_triggers,
    list_views,
)
from src.mcp_postgres.utils.exceptions import (
    MCPPostgresError,
    TableNotFoundError,
)


class TestListTables:
    """Test cases for list_tables function."""

    @pytest.fixture
    def mock_table_rows(self):
        """Mock table data from database query."""
        return [
            {
                "table_name": "users",
                "table_type": "BASE TABLE",
                "table_schema": "public",
                "size_human": "64 kB",
                "size_bytes": 65536,
                "comment": "User accounts table",
                "estimated_rows": 100,
            },
            {
                "table_name": "orders",
                "table_type": "BASE TABLE",
                "table_schema": "public",
                "size_human": "128 kB",
                "size_bytes": 131072,
                "comment": None,
                "estimated_rows": 250,
            },
            {
                "table_name": "user_stats",
                "table_type": "VIEW",
                "table_schema": "public",
                "size_human": None,
                "size_bytes": None,
                "comment": "User statistics view",
                "estimated_rows": None,
            },
        ]

    @pytest.mark.asyncio
    async def test_list_tables_default_schema(self, mock_table_rows):
        """Test listing tables with default public schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_table_rows)

            result = await list_tables()

            assert result["table_count"] == 3
            assert len(result["tables"]) == 3
            assert result["tables"][0]["table_name"] == "users"
            assert result["total_size_bytes"] == 196608  # 65536 + 131072

            # Verify query was called with public schema
            mock_conn.execute_query.assert_called_once()
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["public"]

    @pytest.mark.asyncio
    async def test_list_tables_custom_schema(self, mock_table_rows):
        """Test listing tables with custom schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_table_rows)

            result = await list_tables(schema_name="analytics")

            assert result["table_count"] == 3

            # Verify query was called with custom schema
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["analytics"]

    @pytest.mark.asyncio
    async def test_list_tables_invalid_schema_name(self):
        """Test listing tables with invalid schema name."""
        with pytest.raises(MCPPostgresError):
            await list_tables(schema_name="invalid-schema!")

    @pytest.mark.asyncio
    async def test_list_tables_database_error(self):
        """Test handling database errors."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(
                side_effect=Exception("Database connection failed")
            )

            with pytest.raises(MCPPostgresError):
                await list_tables()


class TestDescribeTable:
    """Test cases for describe_table function."""

    @pytest.fixture
    def mock_column_rows(self):
        """Mock column data from database query."""
        return [
            {
                "column_name": "id",
                "data_type": "integer",
                "character_maximum_length": None,
                "numeric_precision": 32,
                "numeric_scale": 0,
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq'::regclass)",
                "ordinal_position": 1,
                "is_primary_key": True,
                "is_foreign_key": False,
                "foreign_table_name": None,
                "foreign_column_name": None,
                "comment": "Primary key",
            },
            {
                "column_name": "email",
                "data_type": "character varying",
                "character_maximum_length": 255,
                "numeric_precision": None,
                "numeric_scale": None,
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 2,
                "is_primary_key": False,
                "is_foreign_key": False,
                "foreign_table_name": None,
                "foreign_column_name": None,
                "comment": "User email address",
            },
            {
                "column_name": "profile_id",
                "data_type": "integer",
                "character_maximum_length": None,
                "numeric_precision": 32,
                "numeric_scale": 0,
                "is_nullable": "YES",
                "column_default": None,
                "ordinal_position": 3,
                "is_primary_key": False,
                "is_foreign_key": True,
                "foreign_table_name": "profiles",
                "foreign_column_name": "id",
                "comment": None,
            },
        ]

    @pytest.mark.asyncio
    async def test_describe_table_success(self, mock_column_rows):
        """Test successful table description."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # Mock table exists check
            mock_conn.execute_query = AsyncMock(side_effect=[True, mock_column_rows])

            result = await describe_table("users")

            assert result["table_name"] == "users"
            assert result["column_count"] == 3
            assert len(result["columns"]) == 3
            assert result["metadata"]["has_primary_key"] is True

            # Verify both queries were called
            assert mock_conn.execute_query.call_count == 2

    @pytest.mark.asyncio
    async def test_describe_table_not_found(self):
        """Test describing non-existent table."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # Mock table doesn't exist
            mock_conn.execute_query = AsyncMock(return_value=False)

            with pytest.raises(TableNotFoundError):
                await describe_table("nonexistent_table")

    @pytest.mark.asyncio
    async def test_describe_table_custom_schema(self, mock_column_rows):
        """Test describing table in custom schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(side_effect=[True, mock_column_rows])

            result = await describe_table("users", schema_name="analytics")

            assert result["table_name"] == "users"

            # Verify schema was passed to queries
            calls = mock_conn.execute_query.call_args_list
            assert calls[0][0][1] == ["analytics", "users"]
            assert calls[1][0][1] == ["analytics", "users"]

    @pytest.mark.asyncio
    async def test_describe_table_invalid_name(self):
        """Test describing table with invalid name."""
        with pytest.raises(MCPPostgresError):
            await describe_table("invalid-table!")


class TestListIndexes:
    """Test cases for list_indexes function."""

    @pytest.fixture
    def mock_index_rows(self):
        """Mock index data from database query."""
        return [
            {
                "index_name": "users_pkey",
                "table_name": "users",
                "schema_name": "public",
                "index_definition": "CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)",
                "size_human": "16 kB",
                "size_bytes": 16384,
                "index_type": "PRIMARY KEY",
                "scans": 1500,
                "tuples_read": 1500,
                "tuples_fetched": 1500,
            },
            {
                "index_name": "users_email_idx",
                "table_name": "users",
                "schema_name": "public",
                "index_definition": "CREATE INDEX users_email_idx ON public.users USING btree (email)",
                "size_human": "8 kB",
                "size_bytes": 8192,
                "index_type": "INDEX",
                "scans": 250,
                "tuples_read": 300,
                "tuples_fetched": 250,
            },
        ]

    @pytest.mark.asyncio
    async def test_list_indexes_all_tables(self, mock_index_rows):
        """Test listing all indexes in schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_index_rows)

            result = await list_indexes()

            assert result["index_count"] == 2
            assert len(result["indexes"]) == 2
            assert result["total_size_bytes"] == 24576  # 16384 + 8192
            assert result["total_scans"] == 1750  # 1500 + 250
            assert result["metadata"]["schema_name"] == "public"
            assert result["metadata"]["table_name"] is None

    @pytest.mark.asyncio
    async def test_list_indexes_specific_table(self, mock_index_rows):
        """Test listing indexes for specific table."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_index_rows)

            result = await list_indexes(table_name="users")

            assert result["index_count"] == 2
            assert result["metadata"]["table_name"] == "users"

            # Verify query was called with table name
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["public", "users"]

    @pytest.mark.asyncio
    async def test_list_indexes_custom_schema(self, mock_index_rows):
        """Test listing indexes in custom schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_index_rows)

            result = await list_indexes(schema_name="analytics")

            assert result["metadata"]["schema_name"] == "analytics"

            # Verify schema was passed to query
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["analytics"]


class TestListConstraints:
    """Test cases for list_constraints function."""

    @pytest.fixture
    def mock_constraint_rows(self):
        """Mock constraint data from database query."""
        return [
            {
                "constraint_name": "users_pkey",
                "table_name": "users",
                "table_schema": "public",
                "constraint_type": "PRIMARY KEY",
                "columns": "id",
                "foreign_table_name": None,
                "foreign_column_name": None,
                "update_rule": None,
                "delete_rule": None,
                "check_clause": None,
            },
            {
                "constraint_name": "users_email_key",
                "table_name": "users",
                "table_schema": "public",
                "constraint_type": "UNIQUE",
                "columns": "email",
                "foreign_table_name": None,
                "foreign_column_name": None,
                "update_rule": None,
                "delete_rule": None,
                "check_clause": None,
            },
            {
                "constraint_name": "orders_user_id_fkey",
                "table_name": "orders",
                "table_schema": "public",
                "constraint_type": "FOREIGN KEY",
                "columns": "user_id",
                "foreign_table_name": "users",
                "foreign_column_name": "id",
                "update_rule": "NO ACTION",
                "delete_rule": "CASCADE",
                "check_clause": None,
            },
        ]

    @pytest.mark.asyncio
    async def test_list_constraints_all_tables(self, mock_constraint_rows):
        """Test listing all constraints in schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_constraint_rows)

            result = await list_constraints()

            assert result["constraint_count"] == 3
            assert len(result["constraints"]) == 3
            assert result["constraint_types"]["PRIMARY KEY"] == 1
            assert result["constraint_types"]["UNIQUE"] == 1
            assert result["constraint_types"]["FOREIGN KEY"] == 1
            assert result["metadata"]["has_foreign_keys"] is True

    @pytest.mark.asyncio
    async def test_list_constraints_specific_table(self, mock_constraint_rows):
        """Test listing constraints for specific table."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # Filter to just users table constraints
            users_constraints = [
                c for c in mock_constraint_rows if c["table_name"] == "users"
            ]
            mock_conn.execute_query = AsyncMock(return_value=users_constraints)

            result = await list_constraints(table_name="users")

            assert result["constraint_count"] == 2
            assert result["metadata"]["table_name"] == "users"

            # Verify query was called with table name
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["public", "users"]


class TestListViews:
    """Test cases for list_views function."""

    @pytest.fixture
    def mock_view_rows(self):
        """Mock view data from database query."""
        return [
            {
                "view_name": "user_stats",
                "schema_name": "public",
                "view_definition": "SELECT u.id, u.email, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.email",
                "check_option": "NONE",
                "is_updatable": "NO",
                "is_insertable_into": "NO",
                "is_trigger_updatable": "NO",
                "is_trigger_deletable": "NO",
                "is_trigger_insertable_into": "NO",
                "comment": "User statistics view",
            },
        ]

    @pytest.fixture
    def mock_dependency_rows(self):
        """Mock view dependency data."""
        return [
            {
                "referenced_schema": "public",
                "referenced_table": "users",
                "referenced_type": "r",
            },
            {
                "referenced_schema": "public",
                "referenced_table": "orders",
                "referenced_type": "r",
            },
        ]

    @pytest.mark.asyncio
    async def test_list_views_success(self, mock_view_rows, mock_dependency_rows):
        """Test successful view listing with dependencies."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(
                side_effect=[mock_view_rows, mock_dependency_rows]
            )

            result = await list_views()

            assert result["view_count"] == 1
            assert len(result["views"]) == 1
            assert result["views"][0]["view_name"] == "user_stats"
            assert len(result["views"][0]["dependencies"]) == 2
            assert result["metadata"]["updatable_views"] == 0
            assert result["metadata"]["has_dependencies"] is True

    @pytest.mark.asyncio
    async def test_list_views_dependency_error(self, mock_view_rows):
        """Test view listing when dependency query fails."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # First call succeeds, second fails
            mock_conn.execute_query = AsyncMock(
                side_effect=[mock_view_rows, Exception("Dependency query failed")]
            )

            result = await list_views()

            assert result["view_count"] == 1
            assert result["views"][0]["dependencies"] == []  # Empty due to error


class TestListFunctions:
    """Test cases for list_functions function."""

    @pytest.fixture
    def mock_function_rows(self):
        """Mock function data from database query."""
        return [
            {
                "function_name": "calculate_total",
                "schema_name": "public",
                "routine_type": "FUNCTION",
                "return_type": "numeric",
                "function_definition": "BEGIN RETURN price * quantity; END;",
                "language": "plpgsql",
                "is_deterministic": "YES",
                "sql_data_access": "READS SQL DATA",
                "is_null_call": "YES",
                "comment": "Calculate order total",
                "parameters": "price numeric, quantity integer",
            },
            {
                "function_name": "update_user_stats",
                "schema_name": "public",
                "routine_type": "PROCEDURE",
                "return_type": None,
                "function_definition": "BEGIN UPDATE user_stats SET last_updated = NOW(); END;",
                "language": "plpgsql",
                "is_deterministic": "NO",
                "sql_data_access": "MODIFIES SQL DATA",
                "is_null_call": "NO",
                "comment": None,
                "parameters": None,
            },
        ]

    @pytest.mark.asyncio
    async def test_list_functions_success(self, mock_function_rows):
        """Test successful function listing."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_function_rows)

            result = await list_functions()

            assert result["function_count"] == 2
            assert len(result["functions"]) == 2
            assert result["function_types"]["FUNCTION"] == 1
            assert result["function_types"]["PROCEDURE"] == 1
            assert result["languages"]["plpgsql"] == 2
            assert result["metadata"]["has_procedures"] is True
            assert result["metadata"]["has_plpgsql_functions"] is True


class TestListTriggers:
    """Test cases for list_triggers function."""

    @pytest.fixture
    def mock_trigger_rows(self):
        """Mock trigger data from database query."""
        return [
            {
                "trigger_name": "update_modified_time",
                "table_name": "users",
                "schema_name": "public",
                "trigger_schema": "public",
                "trigger_event": "UPDATE",
                "action_timing": "BEFORE",
                "action_orientation": "ROW",
                "action_statement": "EXECUTE FUNCTION update_modified_column()",
                "action_condition": None,
                "trigger_definition": "CREATE TRIGGER update_modified_time BEFORE UPDATE ON public.users FOR EACH ROW EXECUTE FUNCTION update_modified_column()",
            },
            {
                "trigger_name": "log_user_changes",
                "table_name": "users",
                "schema_name": "public",
                "trigger_schema": "public",
                "trigger_event": "INSERT",
                "action_timing": "AFTER",
                "action_orientation": "ROW",
                "action_statement": "EXECUTE FUNCTION log_changes()",
                "action_condition": None,
                "trigger_definition": "CREATE TRIGGER log_user_changes AFTER INSERT ON public.users FOR EACH ROW EXECUTE FUNCTION log_changes()",
            },
        ]

    @pytest.mark.asyncio
    async def test_list_triggers_all_tables(self, mock_trigger_rows):
        """Test listing all triggers in schema."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_trigger_rows)

            result = await list_triggers()

            assert result["trigger_count"] == 2
            assert len(result["triggers"]) == 2
            assert result["event_types"]["UPDATE"] == 1
            assert result["event_types"]["INSERT"] == 1
            assert result["timing_types"]["BEFORE"] == 1
            assert result["timing_types"]["AFTER"] == 1
            assert result["metadata"]["has_before_triggers"] is True
            assert result["metadata"]["has_after_triggers"] is True

    @pytest.mark.asyncio
    async def test_list_triggers_specific_table(self, mock_trigger_rows):
        """Test listing triggers for specific table."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query = AsyncMock(return_value=mock_trigger_rows)

            result = await list_triggers(table_name="users")

            assert result["trigger_count"] == 2
            assert result["metadata"]["table_name"] == "users"

            # Verify query was called with table name
            args = mock_conn.execute_query.call_args
            assert args[0][1] == ["public", "users"]


class TestListSequences:
    """Test cases for list_sequences function."""

    @pytest.fixture
    def mock_sequence_rows(self):
        """Mock sequence data from database query."""
        return [
            {
                "sequence_name": "users_id_seq",
                "schema_name": "public",
                "data_type": "bigint",
                "numeric_precision": 64,
                "numeric_scale": 0,
                "start_value": 1,
                "minimum_value": 1,
                "maximum_value": 9223372036854775807,
                "increment": 1,
                "cycle_option": "NO",
                "last_value": 150,
                "remaining_values": 9223372036854775657,
                "comment": "Primary key sequence for users table",
            },
            {
                "sequence_name": "order_number_seq",
                "schema_name": "public",
                "data_type": "integer",
                "numeric_precision": 32,
                "numeric_scale": 0,
                "start_value": 1000,
                "minimum_value": 1000,
                "maximum_value": 999999,
                "increment": 1,
                "cycle_option": "YES",
                "last_value": 5000,
                "remaining_values": 994999,
                "comment": None,
            },
        ]

    @pytest.fixture
    def mock_ownership_rows(self):
        """Mock sequence ownership data."""
        return [
            {
                "table_name": "users",
                "column_name": "id",
                "table_schema": "public",
            },
        ]

    @pytest.mark.asyncio
    async def test_list_sequences_success(
        self, mock_sequence_rows, mock_ownership_rows
    ):
        """Test successful sequence listing with ownership info."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # First call returns sequences, subsequent calls return ownership info
            mock_conn.execute_query = AsyncMock(
                side_effect=[
                    mock_sequence_rows,
                    mock_ownership_rows,  # ownership for users_id_seq
                    [],  # no ownership for order_number_seq
                ]
            )

            result = await list_sequences()

            assert result["sequence_count"] == 2
            assert len(result["sequences"]) == 2
            assert result["cycling_sequences"] == 1
            assert result["sequences_with_values"] == 2
            assert result["metadata"]["has_cycling_sequences"] is True
            assert result["metadata"]["has_owned_sequences"] is True

            # Check ownership info was added
            assert len(result["sequences"][0]["owned_by"]) == 1
            assert result["sequences"][0]["owned_by"][0]["table_name"] == "users"

    @pytest.mark.asyncio
    async def test_list_sequences_ownership_error(self, mock_sequence_rows):
        """Test sequence listing when ownership query fails."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # First call succeeds, ownership calls fail
            mock_conn.execute_query = AsyncMock(
                side_effect=[
                    mock_sequence_rows,
                    Exception("Ownership query failed"),
                    Exception("Ownership query failed"),
                ]
            )

            result = await list_sequences()

            assert result["sequence_count"] == 2
            # Both sequences should have empty owned_by due to errors
            assert result["sequences"][0]["owned_by"] == []
            assert result["sequences"][1]["owned_by"] == []


class TestErrorHandling:
    """Test error handling across all schema tools."""

    @pytest.mark.asyncio
    async def test_connection_error_handling(self):
        """Test handling of connection errors."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            mock_conn.execute_query.side_effect = Exception("Connection failed")

            with pytest.raises(MCPPostgresError):
                await list_tables()

            with pytest.raises(MCPPostgresError):
                await list_indexes()

            with pytest.raises(MCPPostgresError):
                await list_constraints()

    @pytest.mark.asyncio
    async def test_validation_error_propagation(self):
        """Test that validation errors are properly propagated."""
        # Test invalid table names
        with pytest.raises(MCPPostgresError):
            await describe_table("invalid-table!")

        with pytest.raises(MCPPostgresError):
            await list_indexes(table_name="invalid-table!")

        with pytest.raises(MCPPostgresError):
            await list_triggers(table_name="invalid-table!")

    @pytest.mark.asyncio
    async def test_postgres_error_conversion(self):
        """Test conversion of PostgreSQL errors to MCP errors."""
        with patch(
            "src.mcp_postgres.tools.schema_tools.connection_manager"
        ) as mock_conn:
            # Mock a PostgreSQL error with sqlstate
            pg_error = Exception("relation does not exist")
            pg_error.sqlstate = "42P01"
            mock_conn.execute_query.side_effect = pg_error

            with pytest.raises(MCPPostgresError):
                await list_tables()
