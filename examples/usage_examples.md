# MCP Postgres Usage Examples

This document provides practical examples of using the MCP Postgres server with various MCP clients.

## Basic Setup

### 1. Environment Configuration

Create a `.env` file:
```bash
DATABASE_URL="postgresql://postgres:password@localhost:5432/myapp"
LOG_LEVEL="INFO"
```

### 2. Start the Server

```bash
# Using uv
uv run python -m mcp_postgres

# Using installed package
mcp-postgres

# Development mode
uv run python -m mcp_postgres --dev
```

## Claude Desktop Integration

### Configuration

Add to your Claude Desktop config file:

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uv",
      "args": ["run", "python", "-m", "mcp_postgres"],
      "cwd": "/path/to/mcp-postgres",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "LOG_LEVEL": "INFO"
      }
    }
  }
}
```

### Example Conversations

#### Database Schema Exploration

**User**: "What tables are in my database?"

**Claude**: I'll check what tables are available in your database.

*Uses `list_tables` tool*

**User**: "Show me the structure of the users table"

**Claude**: Let me get the detailed structure of the users table.

*Uses `describe_table` tool with `table_name: "users"`*

#### Data Analysis

**User**: "Find duplicate email addresses in the users table"

**Claude**: I'll check for duplicate email addresses in your users table.

*Uses `find_duplicates` tool with `table_name: "users"` and `columns: ["email"]`*

**User**: "Analyze the distribution of ages in the users table"

**Claude**: Let me analyze the age column statistics.

*Uses `analyze_column` tool with `table_name: "users"` and `column_name: "age"`*

#### Query Execution

**User**: "Show me all users created in the last 30 days"

**Claude**: I'll query for users created in the last 30 days.

*Uses `execute_query` tool with:*
```sql
SELECT id, name, email, created_at
FROM users
WHERE created_at >= NOW() - INTERVAL '30 days'
ORDER BY created_at DESC
```

## Python Client Example

```python
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    # Configure server parameters
    server_params = StdioServerParameters(
        command="python",
        args=["-m", "mcp_postgres"],
        env={
            "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
            "LOG_LEVEL": "DEBUG"
        }
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the connection
            await session.initialize()

            # List available tools
            tools_result = await session.list_tools()
            print(f"Available tools: {len(tools_result.tools)}")

            # Execute a query
            query_result = await session.call_tool(
                "execute_query",
                arguments={
                    "query": "SELECT COUNT(*) as user_count FROM users WHERE active = $1",
                    "parameters": [True]
                }
            )
            print("Query result:", query_result)

            # Get table structure
            table_info = await session.call_tool(
                "describe_table",
                arguments={"table_name": "users"}
            )
            print("Table structure:", table_info)

            # Find duplicates
            duplicates = await session.call_tool(
                "find_duplicates",
                arguments={
                    "table_name": "customers",
                    "columns": ["email"]
                }
            )
            print("Duplicate emails:", duplicates)

if __name__ == "__main__":
    asyncio.run(main())
```

## Common Use Cases

### 1. Database Health Check

```python
# Check database info
db_info = await session.call_tool("get_database_info")

# Monitor connections
connections = await session.call_tool("monitor_connections")

# Get table statistics
stats = await session.call_tool(
    "get_table_stats",
    arguments={"table_name": "orders"}
)
```

### 2. Data Quality Analysis

```python
# Check data integrity
integrity = await session.call_tool(
    "check_data_integrity",
    arguments={"table_name": "products"}
)

# Validate constraints
constraints = await session.call_tool(
    "validate_constraints",
    arguments={"table_name": "orders"}
)

# Profile table data
profile = await session.call_tool(
    "profile_table",
    arguments={"table_name": "customers"}
)
```

### 3. Performance Analysis

```python
# Analyze query performance
performance = await session.call_tool(
    "analyze_query_performance",
    arguments={
        "query": "SELECT * FROM orders WHERE customer_id = 123"
    }
)

# Find slow queries
slow_queries = await session.call_tool("find_slow_queries")

# Get table statistics
table_stats = await session.call_tool(
    "get_table_stats",
    arguments={"table_name": "large_table"}
)
```

### 4. Data Export/Import

```python
# Export table to CSV
export_result = await session.call_tool(
    "export_table_csv",
    arguments={
        "table_name": "customers",
        "file_path": "/tmp/customers.csv"
    }
)

# Import CSV data
import_result = await session.call_tool(
    "import_csv_data",
    arguments={
        "table_name": "new_customers",
        "file_path": "/tmp/new_data.csv",
        "has_header": True
    }
)
```

### 5. Code Generation

```python
# Generate DDL
ddl = await session.call_tool(
    "generate_ddl",
    arguments={"table_name": "users"}
)

# Generate INSERT templates
insert_template = await session.call_tool(
    "generate_insert_template",
    arguments={"table_name": "products"}
)

# Generate ORM models
orm_model = await session.call_tool(
    "generate_orm_model",
    arguments={
        "table_name": "orders",
        "model_type": "sqlalchemy"
    }
)
```

## Error Handling

```python
try:
    result = await session.call_tool(
        "execute_query",
        arguments={
            "query": "SELECT * FROM non_existent_table"
        }
    )
except Exception as e:
    print(f"Query failed: {e}")
    # Handle the error appropriately
```

## Best Practices

### 1. Use Parameterized Queries

```python
# Good - parameterized query
result = await session.call_tool(
    "execute_query",
    arguments={
        "query": "SELECT * FROM users WHERE age > $1 AND city = $2",
        "parameters": [25, "New York"]
    }
)

# Avoid - string concatenation (security risk)
# Don't do this in production
```

### 2. Limit Result Sets

```python
# Use LIMIT for large tables
result = await session.call_tool(
    "execute_query",
    arguments={
        "query": "SELECT * FROM large_table LIMIT 100"
    }
)
```

### 3. Handle Transactions

```python
# Use transactions for multiple related operations
result = await session.call_tool(
    "execute_transaction",
    arguments={
        "queries": [
            {
                "query": "INSERT INTO orders (customer_id, total) VALUES ($1, $2)",
                "parameters": [123, 99.99]
            },
            {
                "query": "UPDATE customers SET last_order_date = NOW() WHERE id = $1",
                "parameters": [123]
            }
        ]
    }
)
```

### 4. Monitor Performance

```python
# Regular performance checks
performance = await session.call_tool(
    "analyze_query_performance",
    arguments={
        "query": "SELECT * FROM orders WHERE created_at > $1",
        "parameters": ["2024-01-01"]
    }
)

if performance.get("execution_time", 0) > 1000:  # ms
    print("Query is slow, consider optimization")
```

## Troubleshooting

### Connection Issues

```python
# Test database connection
try:
    db_info = await session.call_tool("get_database_info")
    print("Database connection successful")
except Exception as e:
    print(f"Connection failed: {e}")
```

### Query Debugging

```python
# Use raw query tool for debugging
debug_result = await session.call_tool(
    "execute_raw_query",
    arguments={
        "query": "EXPLAIN ANALYZE SELECT * FROM users WHERE active = true"
    }
)
```

### Performance Issues

```python
# Check for slow queries
slow_queries = await session.call_tool("find_slow_queries")

# Analyze table statistics
stats = await session.call_tool(
    "get_table_stats",
    arguments={"table_name": "problematic_table"}
)

# Consider vacuuming if needed
vacuum_result = await session.call_tool(
    "vacuum_table",
    arguments={"table_name": "large_table"}
)
```
