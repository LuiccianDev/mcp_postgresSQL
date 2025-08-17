"""Unit tests for admin tools module."""

from unittest.mock import AsyncMock, patch

import pytest

from src.mcp_postgres.tools.admin_tools import (
    get_database_info,
    monitor_connections,
    reindex_table,
    vacuum_table,
)


class TestGetDatabaseInfo:
    """Test cases for get_database_info function."""

    @pytest.mark.asyncio
    async def test_get_database_info_success(self):
        """Test successful database info retrieval."""
        # Mock database responses
        mock_version = "PostgreSQL 15.4 on x86_64-pc-linux-gnu"
        mock_size = 52428800  # 50MB
        mock_settings = {
            "database_name": "testdb",
            "current_user": "postgres",
            "session_user": "postgres",
            "current_schema": "public",
            "server_address": "127.0.0.1",
            "server_port": 5432,
            "backend_pid": 12345,
            "is_in_recovery": False,
        }
        mock_max_conn = 100
        mock_current_conn = 5
        mock_stats = {
            "active_connections": 5,
            "transactions_committed": 1000,
            "transactions_rolled_back": 10,
            "blocks_read": 500,
            "blocks_hit": 4500,
            "tuples_returned": 10000,
            "tuples_fetched": 8000,
            "tuples_inserted": 100,
            "tuples_updated": 50,
            "tuples_deleted": 5,
        }

        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            # Setup mock responses for different queries
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_version,  # version query
                mock_size,     # size query
                mock_settings, # settings query
                mock_max_conn, # max connections query
                mock_current_conn, # current connections query
                mock_stats,    # stats query
            ]

            result = await get_database_info()

            # Assertions
            assert result["success"] is True
            assert "data" in result
            data = result["data"]

            assert data["version"] == mock_version
            assert data["database_name"] == "testdb"
            assert data["current_user"] == "postgres"
            assert data["database_size_bytes"] == mock_size
            assert data["database_size_human"] == "50.0 MB"
            assert data["max_connections"] == mock_max_conn
            assert data["current_connections"] == mock_current_conn
            assert data["connection_usage_percent"] == 5.0
            assert data["cache_hit_ratio"] == 90.0  # 4500/(4500+500)*100
            assert "execution_time_ms" in data
            assert "statistics" in data

            # Verify all queries were called
            assert mock_conn_mgr.execute_query.call_count == 6

    @pytest.mark.asyncio
    async def test_get_database_info_database_error(self):
        """Test error handling for database errors."""
        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            result = await get_database_info()

            assert "error" in result
            assert result["error"]["code"] in ["DATABASE_ERROR", "QUERY_EXECUTION_ERROR"]


class TestMonitorConnections:
    """Test cases for monitor_connections function."""

    @pytest.mark.asyncio
    async def test_monitor_connections_success(self):
        """Test successful connection monitoring."""
        # Mock connection data
        mock_connections = [
            {
                "pid": 12345,
                "username": "postgres",
                "application_name": "psql",
                "client_address": "127.0.0.1",
                "client_port": 54321,
                "backend_start": "2024-01-01T10:00:00",
                "query_start": "2024-01-01T10:05:00",
                "state_change": "2024-01-01T10:05:00",
                "state": "active",
                "query": "SELECT * FROM users",
                "backend_type": "client backend",
                "connection_duration_seconds": 300.0,
                "query_duration_seconds": 60.0,
                "is_active": 1,
            },
            {
                "pid": 12346,
                "username": "app_user",
                "application_name": "myapp",
                "client_address": "192.168.1.100",
                "client_port": 54322,
                "backend_start": "2024-01-01T09:30:00",
                "query_start": None,
                "state_change": "2024-01-01T10:00:00",
                "state": "idle",
                "query": "",
                "backend_type": "client backend",
                "connection_duration_seconds": 1800.0,
                "query_duration_seconds": None,
                "is_active": 0,
            }
        ]

        mock_summary = [
            {"state": "active", "connection_count": 1, "avg_connection_duration": 300.0, "max_connection_duration": 300.0},
            {"state": "idle", "connection_count": 1, "avg_connection_duration": 1800.0, "max_connection_duration": 1800.0},
        ]

        mock_long_queries = [
            {
                "pid": 12347,
                "username": "postgres",
                "query": "SELECT * FROM large_table WHERE complex_condition = true",
                "state": "active",
                "query_duration_seconds": 45.0,
            }
        ]

        mock_blocked_queries = []

        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                mock_connections,    # connections query
                mock_summary,        # summary query
                mock_long_queries,   # long queries query
                mock_blocked_queries, # blocked queries query
            ]

            result = await monitor_connections()

            # Assertions
            assert result["success"] is True
            assert "data" in result
            data = result["data"]

            assert data["connection_count"] == 2
            assert data["active_connections"] == 1
            assert len(data["connections"]) == 2
            assert len(data["summary_by_state"]) == 2
            assert len(data["long_running_queries"]) == 1
            assert len(data["blocked_queries"]) == 0
            assert data["metadata"]["has_long_queries"] is True
            assert data["metadata"]["has_blocked_queries"] is False
            assert "execution_time_ms" in data

            # Check connection formatting
            first_conn = data["connections"][0]
            assert "connection_duration_human" in first_conn
            assert "query_duration_human" in first_conn

            # Verify all queries were called
            assert mock_conn_mgr.execute_query.call_count == 4

    @pytest.mark.asyncio
    async def test_monitor_connections_database_error(self):
        """Test error handling for database errors."""
        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            result = await monitor_connections()

            assert "error" in result
            assert result["error"]["code"] in ["DATABASE_ERROR", "QUERY_EXECUTION_ERROR"]


class TestVacuumTable:
    """Test cases for vacuum_table function."""

    @pytest.mark.asyncio
    async def test_vacuum_table_success_standard(self):
        """Test successful standard vacuum operation."""
        table_name = "test_table"
        mock_table_exists = True
        mock_size_before = 1048576  # 1MB
        mock_size_after = 524288    # 512KB
        mock_vacuum_result = "VACUUM"
        mock_stats = {
            "schemaname": "public",
            "tablename": "test_table",
            "tuples_inserted": 1000,
            "tuples_updated": 100,
            "tuples_deleted": 50,
            "live_tuples": 950,
            "dead_tuples": 0,
            "last_vacuum": "2024-01-01T10:00:00",
            "last_autovacuum": None,
            "last_analyze": "2024-01-01T10:00:00",
            "last_autoanalyze": None,
            "vacuum_count": 5,
            "autovacuum_count": 2,
            "analyze_count": 3,
            "autoanalyze_count": 1,
        }

        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_raw_query = AsyncMock(return_value=mock_vacuum_result)

            # Setup query responses
            mock_conn_mgr.execute_query.side_effect = [
                mock_table_exists,  # table exists check
                mock_size_before,   # size before
                mock_size_after,    # size after
                mock_stats,         # table stats
            ]

            result = await vacuum_table(table_name=table_name, analyze=True, full=False)

            # Assertions
            assert result["success"] is True
            assert "data" in result
            data = result["data"]

            assert data["table_name"] == table_name
            assert data["vacuum_type"] == "STANDARD"
            assert data["analyze_performed"] is True
            assert data["operation_status"] == mock_vacuum_result
            assert data["size_before_bytes"] == mock_size_before
            assert data["size_after_bytes"] == mock_size_after
            assert data["space_reclaimed_bytes"] == mock_size_before - mock_size_after
            assert data["space_reclaimed_percent"] == 50.0
            assert "execution_time_ms" in data
            assert "table_statistics" in data

            # Verify vacuum command was called
            mock_conn_mgr.execute_raw_query.assert_called_once_with(
                "VACUUM ANALYZE test_table", fetch_mode="none"
            )

    @pytest.mark.asyncio
    async def test_vacuum_table_success_full(self):
        """Test successful full vacuum operation."""
        table_name = "test_table"

        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_raw_query = AsyncMock(return_value="VACUUM")

            # Setup basic responses
            mock_conn_mgr.execute_query.side_effect = [
                True,      # table exists
                1000000,   # size before
                800000,    # size after
                {},        # stats
            ]

            result = await vacuum_table(table_name=table_name, analyze=False, full=True)

            assert result["success"] is True
            data = result["data"]
            assert data["vacuum_type"] == "FULL"
            assert data["analyze_performed"] is False

            # Verify full vacuum command was called
            mock_conn_mgr.execute_raw_query.assert_called_once_with(
                "VACUUM FULL test_table", fetch_mode="none"
            )

    @pytest.mark.asyncio
    async def test_vacuum_table_empty_name_error(self):
        """Test error handling for empty table name."""
        result = await vacuum_table(table_name="")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_vacuum_table_invalid_name_error(self):
        """Test error handling for invalid table name."""
        result = await vacuum_table(table_name="test'; DROP TABLE users; --")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "invalid characters" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_vacuum_table_not_exists_error(self):
        """Test error handling for non-existent table."""
        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock(return_value=False)  # table doesn't exist

            result = await vacuum_table(table_name="nonexistent_table")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"
            assert "does not exist" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_vacuum_table_security_error(self):
        """Test error handling for security validation failure."""
        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (False, "Dangerous operation detected")
            mock_conn_mgr.execute_query = AsyncMock(return_value=True)  # table exists

            result = await vacuum_table(table_name="test_table")

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"


class TestReindexTable:
    """Test cases for reindex_table function."""

    @pytest.mark.asyncio
    async def test_reindex_table_success(self):
        """Test successful table reindex operation."""
        table_name = "test_table"
        mock_table_exists = True
        mock_reindex_result = "REINDEX"
        mock_indexes_before = [
            {
                "indexname": "test_table_pkey",
                "indexdef": "CREATE UNIQUE INDEX test_table_pkey ON test_table USING btree (id)",
                "index_size_bytes": 16384,
            },
            {
                "indexname": "test_table_name_idx",
                "indexdef": "CREATE INDEX test_table_name_idx ON test_table USING btree (name)",
                "index_size_bytes": 8192,
            }
        ]
        mock_indexes_after = [
            {
                "indexname": "test_table_pkey",
                "indexdef": "CREATE UNIQUE INDEX test_table_pkey ON test_table USING btree (id)",
                "index_size_bytes": 16384,
            },
            {
                "indexname": "test_table_name_idx",
                "indexdef": "CREATE INDEX test_table_name_idx ON test_table USING btree (name)",
                "index_size_bytes": 8192,
            }
        ]

        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_raw_query = AsyncMock(return_value=mock_reindex_result)

            # Setup query responses
            mock_conn_mgr.execute_query.side_effect = [
                mock_table_exists,     # table exists check
                mock_indexes_before,   # indexes before
                mock_indexes_after,    # indexes after
            ]

            result = await reindex_table(table_name=table_name)

            # Assertions
            assert result["success"] is True
            assert "data" in result
            data = result["data"]

            assert data["table_name"] == table_name
            assert data["index_name"] is None
            assert data["operation_type"] == "TABLE"
            assert data["operation_status"] == mock_reindex_result
            assert data["indexes_processed"] == 2
            assert data["total_size_before_bytes"] == 24576  # 16384 + 8192
            assert data["total_size_after_bytes"] == 24576
            assert data["size_change_bytes"] == 0
            assert "execution_time_ms" in data
            assert "indexes_before" in data
            assert "indexes_after" in data

            # Verify reindex command was called
            mock_conn_mgr.execute_raw_query.assert_called_once_with(
                "REINDEX TABLE test_table", fetch_mode="none"
            )

    @pytest.mark.asyncio
    async def test_reindex_specific_index_success(self):
        """Test successful specific index reindex operation."""
        table_name = "test_table"
        index_name = "test_table_name_idx"

        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_raw_query = AsyncMock(return_value="REINDEX")

            # Setup query responses
            mock_conn_mgr.execute_query.side_effect = [
                True,  # table exists
                True,  # index exists
                [{"indexname": index_name, "indexdef": "CREATE INDEX...", "index_size_bytes": 8192}],  # before
                [{"indexname": index_name, "indexdef": "CREATE INDEX...", "index_size_bytes": 8192}],  # after
            ]

            result = await reindex_table(table_name=table_name, index_name=index_name)

            assert result["success"] is True
            data = result["data"]
            assert data["index_name"] == index_name
            assert data["operation_type"] == "INDEX"

            # Verify specific index reindex command was called
            mock_conn_mgr.execute_raw_query.assert_called_once_with(
                f"REINDEX INDEX {index_name}", fetch_mode="none"
            )

    @pytest.mark.asyncio
    async def test_reindex_table_empty_name_error(self):
        """Test error handling for empty table name."""
        result = await reindex_table(table_name="")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_reindex_table_invalid_name_error(self):
        """Test error handling for invalid table name."""
        result = await reindex_table(table_name="test'; DROP TABLE users; --")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "invalid characters" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_reindex_table_not_exists_error(self):
        """Test error handling for non-existent table."""
        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock(return_value=False)  # table doesn't exist

            result = await reindex_table(table_name="nonexistent_table")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"
            assert "does not exist" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_reindex_index_not_exists_error(self):
        """Test error handling for non-existent index."""
        with patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:
            mock_conn_mgr.execute_query = AsyncMock()
            mock_conn_mgr.execute_query.side_effect = [
                True,   # table exists
                False,  # index doesn't exist
            ]

            result = await reindex_table(table_name="test_table", index_name="nonexistent_idx")

            assert "error" in result
            assert result["error"]["code"] == "VALIDATION_ERROR"
            assert "does not exist" in result["error"]["message"]

    @pytest.mark.asyncio
    async def test_reindex_table_security_error(self):
        """Test error handling for security validation failure."""
        with patch("src.mcp_postgres.tools.admin_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.admin_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (False, "Dangerous operation detected")
            mock_conn_mgr.execute_query = AsyncMock(return_value=True)  # table exists

            result = await reindex_table(table_name="test_table")

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"


class TestAdminToolsSchemas:
    """Test cases for admin tool schemas."""

    def test_get_database_info_schema_structure(self):
        """Test that get_database_info schema has correct structure."""
        from src.mcp_postgres.tools.admin_tools import GET_DATABASE_INFO_SCHEMA

        assert GET_DATABASE_INFO_SCHEMA["name"] == "get_database_info"
        assert "description" in GET_DATABASE_INFO_SCHEMA
        assert "inputSchema" in GET_DATABASE_INFO_SCHEMA

        schema = GET_DATABASE_INFO_SCHEMA["inputSchema"]
        assert schema["type"] == "object"
        assert schema["required"] == []

    def test_monitor_connections_schema_structure(self):
        """Test that monitor_connections schema has correct structure."""
        from src.mcp_postgres.tools.admin_tools import MONITOR_CONNECTIONS_SCHEMA

        assert MONITOR_CONNECTIONS_SCHEMA["name"] == "monitor_connections"
        assert "description" in MONITOR_CONNECTIONS_SCHEMA
        assert "inputSchema" in MONITOR_CONNECTIONS_SCHEMA

        schema = MONITOR_CONNECTIONS_SCHEMA["inputSchema"]
        assert schema["type"] == "object"
        assert schema["required"] == []

    def test_vacuum_table_schema_structure(self):
        """Test that vacuum_table schema has correct structure."""
        from src.mcp_postgres.tools.admin_tools import VACUUM_TABLE_SCHEMA

        assert VACUUM_TABLE_SCHEMA["name"] == "vacuum_table"
        assert "description" in VACUUM_TABLE_SCHEMA
        assert "inputSchema" in VACUUM_TABLE_SCHEMA

        schema = VACUUM_TABLE_SCHEMA["inputSchema"]
        assert schema["type"] == "object"
        assert "table_name" in schema["properties"]
        assert "analyze" in schema["properties"]
        assert "full" in schema["properties"]
        assert schema["required"] == ["table_name"]

    def test_reindex_table_schema_structure(self):
        """Test that reindex_table schema has correct structure."""
        from src.mcp_postgres.tools.admin_tools import REINDEX_TABLE_SCHEMA

        assert REINDEX_TABLE_SCHEMA["name"] == "reindex_table"
        assert "description" in REINDEX_TABLE_SCHEMA
        assert "inputSchema" in REINDEX_TABLE_SCHEMA

        schema = REINDEX_TABLE_SCHEMA["inputSchema"]
        assert schema["type"] == "object"
        assert "table_name" in schema["properties"]
        assert "index_name" in schema["properties"]
        assert schema["required"] == ["table_name"]


if __name__ == "__main__":
    pytest.main([__file__])
