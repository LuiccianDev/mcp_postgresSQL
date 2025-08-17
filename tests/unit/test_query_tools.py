"""Unit tests for query tools module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.mcp_postgres.tools.query_tools import (
    execute_query,
    execute_raw_query,
    execute_transaction,
)


class TestExecuteQuery:
    """Test cases for execute_query function."""

    @pytest.mark.asyncio
    async def test_execute_query_success_all_mode(self):
        """Test successful query execution with fetch_mode='all'."""
        # Mock data - create simple dict-like objects that behave like asyncpg Records
        mock_rows = [
            {"id": 1, "name": "John", "email": "john@example.com"},
            {"id": 2, "name": "Jane", "email": "jane@example.com"}
        ]

        # Create mock records that behave like asyncpg Records
        mock_records = []
        for row in mock_rows:
            mock_record = MagicMock()
            mock_record.keys.return_value = row.keys()
            mock_record.__iter__.return_value = iter(row.items())

            # Create a proper closure for __getitem__
            def make_getitem(row_data):
                def mock_getitem(key):
                    return row_data[key]
                return mock_getitem

            mock_record.__getitem__ = make_getitem(row)
            mock_record.items.return_value = row.items()
            # Add a custom dict conversion that will be used by our modified code
            mock_record._asdict = lambda r=row: r
            mock_records.append(mock_record)

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            # Setup mocks
            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = ["John"]
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_records)

            # Execute test
            result = await execute_query(
                query="SELECT * FROM users WHERE name = $1",
                parameters=["John"],
                fetch_mode="all"
            )

            # Assertions
            assert result["success"] is True
            assert "data" in result
            assert result["data"]["rows"] == mock_rows
            assert result["data"]["columns"] == ["id", "name", "email"]
            assert result["data"]["row_count"] == 2
            assert "execution_time_ms" in result["data"]

            # Verify mocks were called correctly
            mock_validate.assert_called_once_with("SELECT * FROM users WHERE name = $1")
            mock_sanitize.assert_called_once_with(["John"])
            mock_conn_mgr.execute_query.assert_called_once_with(
                query="SELECT * FROM users WHERE name = $1",
                parameters=["John"],
                fetch_mode="all"
            )

    @pytest.mark.asyncio
    async def test_execute_query_success_one_mode(self):
        """Test successful query execution with fetch_mode='one'."""
        mock_row = {"id": 1, "name": "John", "email": "john@example.com"}

        # Create mock record that behaves like asyncpg Record
        # Use a simple class instead of MagicMock to avoid issues with dict()
        class MockRecord:
            def __init__(self, data):
                self._data = data

            def keys(self):
                return self._data.keys()

            def __iter__(self):
                return iter(self._data.items())

            def __getitem__(self, key):
                return self._data[key]

            def items(self):
                return self._data.items()

            def _asdict(self):
                return self._data

        mock_record = MockRecord(mock_row)

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = [1]
            mock_conn_mgr.execute_query = AsyncMock(return_value=mock_record)

            result = await execute_query(
                query="SELECT * FROM users WHERE id = $1",
                parameters=[1],
                fetch_mode="one"
            )

            assert result["success"] is True
            assert result["data"]["rows"] == [mock_row]
            assert result["data"]["columns"] == ["id", "name", "email"]
            assert result["data"]["row_count"] == 1

    @pytest.mark.asyncio
    async def test_execute_query_success_val_mode(self):
        """Test successful query execution with fetch_mode='val'."""
        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = []
            mock_conn_mgr.execute_query = AsyncMock(return_value=42)

            result = await execute_query(
                query="SELECT COUNT(*) FROM users",
                parameters=[],
                fetch_mode="val"
            )

            assert result["success"] is True
            assert result["data"]["value"] == 42
            assert result["data"]["metadata"]["has_value"] is True

    @pytest.mark.asyncio
    async def test_execute_query_success_none_mode(self):
        """Test successful query execution with fetch_mode='none'."""
        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = ["John", "john@example.com"]
            mock_conn_mgr.execute_query = AsyncMock(return_value="INSERT 0 1")

            result = await execute_query(
                query="INSERT INTO users (name, email) VALUES ($1, $2)",
                parameters=["John", "john@example.com"],
                fetch_mode="none"
            )

            assert result["success"] is True
            assert result["data"]["status"] == "INSERT 0 1"
            assert result["data"]["metadata"]["operation_completed"] is True

    @pytest.mark.asyncio
    async def test_execute_query_empty_query_error(self):
        """Test error handling for empty query."""
        result = await execute_query(query="", parameters=[])

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_query_invalid_fetch_mode_error(self):
        """Test error handling for invalid fetch_mode."""
        result = await execute_query(
            query="SELECT * FROM users",
            parameters=[],
            fetch_mode="invalid"
        )

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "fetch_mode" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_query_security_validation_error(self):
        """Test error handling for security validation failure."""
        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate:
            mock_validate.return_value = (False, "Dangerous query pattern detected")

            result = await execute_query(
                query="SELECT * FROM users; DROP TABLE users;",
                parameters=[]
            )

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"
            assert "security validation failed" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_query_database_error(self):
        """Test error handling for database execution errors."""
        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = []
            mock_conn_mgr.execute_query = AsyncMock(side_effect=Exception("Database connection failed"))

            result = await execute_query(
                query="SELECT * FROM users",
                parameters=[]
            )

            assert "error" in result


class TestExecuteRawQuery:
    """Test cases for execute_raw_query function."""

    @pytest.mark.asyncio
    async def test_execute_raw_query_success(self):
        """Test successful raw query execution."""
        mock_rows = [{"count": 5}]

        # Create mock records that behave like asyncpg Records
        mock_records = []
        for row in mock_rows:
            mock_record = MagicMock()
            mock_record.keys.return_value = row.keys()
            mock_record.__iter__.return_value = iter(row.items())

            # Create a proper closure for __getitem__
            def make_getitem(row_data):
                def mock_getitem(key):
                    return row_data[key]
                return mock_getitem

            mock_record.__getitem__ = make_getitem(row)
            mock_record.items.return_value = row.items()
            mock_record._asdict = lambda r=row: r
            mock_records.append(mock_record)

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_conn_mgr.execute_raw_query = AsyncMock(return_value=mock_records)

            result = await execute_raw_query(
                query="SELECT COUNT(*) as count FROM users",
                fetch_mode="all"
            )

            assert result["success"] is True
            assert result["data"]["rows"] == mock_rows
            assert "security_warning" in result["data"]
            assert "parameter binding" in result["data"]["security_warning"]

    @pytest.mark.asyncio
    async def test_execute_raw_query_empty_query_error(self):
        """Test error handling for empty raw query."""
        result = await execute_raw_query(query="")

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"

    @pytest.mark.asyncio
    async def test_execute_raw_query_security_error(self):
        """Test error handling for security validation failure in raw query."""
        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate:
            mock_validate.return_value = (False, "Dangerous pattern detected")

            result = await execute_raw_query(query="SELECT * FROM pg_shadow")

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"


class TestExecuteTransaction:
    """Test cases for execute_transaction function."""

    @pytest.mark.asyncio
    async def test_execute_transaction_success(self):
        """Test successful transaction execution."""
        queries = [
            {
                "query": "INSERT INTO users (name, email) VALUES ($1, $2)",
                "parameters": ["John", "john@example.com"],
                "fetch_mode": "none"
            },
            {
                "query": "SELECT id FROM users WHERE email = $1",
                "parameters": ["john@example.com"],
                "fetch_mode": "val"
            }
        ]

        mock_results = ["INSERT 0 1", 123]

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.side_effect = lambda params: params  # Return params unchanged
            mock_conn_mgr.execute_transaction = AsyncMock(return_value=mock_results)

            result = await execute_transaction(queries=queries)

            assert result["success"] is True
            assert result["data"]["query_count"] == 2
            assert result["data"]["metadata"]["transaction_completed"] is True
            assert len(result["data"]["transaction_results"]) == 2

            # Check first query result (INSERT)
            first_result = result["data"]["transaction_results"][0]
            assert first_result["query_index"] == 0
            assert first_result["status"] == "INSERT 0 1"

            # Check second query result (SELECT val)
            second_result = result["data"]["transaction_results"][1]
            assert second_result["query_index"] == 1
            assert second_result["value"] == 123

    @pytest.mark.asyncio
    async def test_execute_transaction_empty_queries_error(self):
        """Test error handling for empty queries list."""
        result = await execute_transaction(queries=[])

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "empty" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_transaction_invalid_query_format_error(self):
        """Test error handling for invalid query format."""
        queries = [
            "invalid query format"  # Should be dict, not string
        ]

        result = await execute_transaction(queries=queries)

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "dictionary" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_transaction_missing_query_error(self):
        """Test error handling for missing query field."""
        queries = [
            {
                "parameters": ["test"],
                "fetch_mode": "all"
                # Missing "query" field
            }
        ]

        result = await execute_transaction(queries=queries)

        assert "error" in result
        assert result["error"]["code"] == "VALIDATION_ERROR"
        assert "missing" in result["error"]["message"].lower()

    @pytest.mark.asyncio
    async def test_execute_transaction_security_validation_error(self):
        """Test error handling for security validation failure in transaction."""
        queries = [
            {
                "query": "SELECT * FROM users; DROP TABLE users;",
                "parameters": [],
                "fetch_mode": "all"
            }
        ]

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate:
            mock_validate.return_value = (False, "Dangerous pattern detected")

            result = await execute_transaction(queries=queries)

            assert "error" in result
            assert result["error"]["code"] == "SECURITY_ERROR"

    @pytest.mark.asyncio
    async def test_execute_transaction_database_error(self):
        """Test error handling for database execution errors in transaction."""
        queries = [
            {
                "query": "INSERT INTO users (name, email) VALUES ($1, $2)",
                "parameters": ["John", "john@example.com"],
                "fetch_mode": "none"
            }
        ]

        with patch("src.mcp_postgres.tools.query_tools.validate_query_permissions") as mock_validate, \
             patch("src.mcp_postgres.tools.query_tools.sanitize_parameters") as mock_sanitize, \
             patch("src.mcp_postgres.tools.query_tools.connection_manager") as mock_conn_mgr:

            mock_validate.return_value = (True, None)
            mock_sanitize.return_value = ["John", "john@example.com"]
            mock_conn_mgr.execute_transaction = AsyncMock(side_effect=Exception("Transaction failed"))

            result = await execute_transaction(queries=queries)

            assert "error" in result


class TestQueryToolsSchemas:
    """Test cases for query tool schemas."""

    def test_execute_query_schema_structure(self):
        """Test that execute_query schema has correct structure."""
        from src.mcp_postgres.tools.query_tools import EXECUTE_QUERY_SCHEMA

        assert EXECUTE_QUERY_SCHEMA["name"] == "execute_query"
        assert "description" in EXECUTE_QUERY_SCHEMA
        assert "inputSchema" in EXECUTE_QUERY_SCHEMA

        schema = EXECUTE_QUERY_SCHEMA["inputSchema"]
        assert schema["type"] == "object"
        assert "query" in schema["properties"]
        assert "parameters" in schema["properties"]
        assert "fetch_mode" in schema["properties"]
        assert schema["required"] == ["query"]

    def test_execute_raw_query_schema_structure(self):
        """Test that execute_raw_query schema has correct structure."""
        from src.mcp_postgres.tools.query_tools import EXECUTE_RAW_QUERY_SCHEMA

        assert EXECUTE_RAW_QUERY_SCHEMA["name"] == "execute_raw_query"
        assert "WARNING" in EXECUTE_RAW_QUERY_SCHEMA["description"]
        assert "inputSchema" in EXECUTE_RAW_QUERY_SCHEMA

        schema = EXECUTE_RAW_QUERY_SCHEMA["inputSchema"]
        assert "query" in schema["properties"]
        assert "fetch_mode" in schema["properties"]
        assert schema["required"] == ["query"]

    def test_execute_transaction_schema_structure(self):
        """Test that execute_transaction schema has correct structure."""
        from src.mcp_postgres.tools.query_tools import EXECUTE_TRANSACTION_SCHEMA

        assert EXECUTE_TRANSACTION_SCHEMA["name"] == "execute_transaction"
        assert "transaction" in EXECUTE_TRANSACTION_SCHEMA["description"]
        assert "inputSchema" in EXECUTE_TRANSACTION_SCHEMA

        schema = EXECUTE_TRANSACTION_SCHEMA["inputSchema"]
        assert "queries" in schema["properties"]
        assert schema["properties"]["queries"]["type"] == "array"
        assert schema["required"] == ["queries"]


if __name__ == "__main__":
    pytest.main([__file__])
