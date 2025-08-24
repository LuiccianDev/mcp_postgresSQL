<div align="center">
  <h1>MCP Postgres</h1>

  <p>
    <em>A comprehensive Model Context Protocol (MCP) server for PostgreSQL database interactions.</em>
  </p>

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP Compatible](https://img.shields.io/badge/MCP-Compatible-brightgreen)](https://modelcontextprotocol.io)

</div>

## Features

- **Query Execution**: Execute SQL queries with parameter binding and transaction support
- **Schema Management**: Inspect database structure, tables, views, indexes, and constraints
- **Data Analysis**: Statistical analysis, duplicate detection, and column profiling
- **Data Management**: CRUD operations with bulk insert capabilities
- **Performance Tools**: Query analysis, slow query detection, and table statistics
- **Backup & Restore**: CSV export/import and table backup functionality
- **Administration**: Database info, connection monitoring, vacuum, and reindexing
- **Code Generation**: Generate SQL DDL, insert templates, and ORM model classes
- **Validation Tools**: Data integrity checks and constraint validation
- **Relation Tools**: Foreign key analysis and referential integrity

## Installation

### Prerequisites

- Python 3.13 or higher
- PostgreSQL database (local or remote)
- `uv` package manager (recommended) or `pip`

### Option 1: Development Setup (Cloned Project)

**Use this approach if you want to:**

- Modify or contribute to the code
- Use the latest development features
- Debug or customize the server behavior

```bash
# Clone the repository
git clone <repository-url>
cd mcp-postgres

# Install dependencies
uv sync

# Install the package in development mode
uv pip install -e .

# Install pre-commit hooks (for development)
uv run pre-commit install
```

### Option 2: Package Installation

**Use this approach if you want to:**

- Simply use the server without modifications
- Have a cleaner installation
- Use a stable released version

```bash
# Install from PyPI (when available)
pip install mcp-postgres

# Or install with uv
uv add mcp-postgres
```

### Which Option Should I Choose?

| Feature | Cloned Project | Installed Package |
|---------|----------------|-------------------|
| **Ease of setup** | Moderate | Easy |
| **Customization** | Full access | Limited |
| **Updates** | Manual git pull | Package manager |
| **Development** | Full development environment | Not suitable |
| **Stability** | Latest code (may be unstable) | Released versions |
| **Disk space** | More (includes dev dependencies) | Less |

## Configuration

### Environment Variables

Create a `.env` file or set the following environment variables:

```bash
# Database connection (required)
DATABASE_URL="postgresql://username:password@localhost:5432/database_name"

# Or individual components
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_DATABASE="your_database"
POSTGRES_USERNAME="your_username"
POSTGRES_PASSWORD="your_password"

# Optional configuration
POSTGRES_POOL_SIZE="10"
POSTGRES_MAX_OVERFLOW="20"
LOG_LEVEL="INFO"
QUERY_TIMEOUT="30"
```

### Database Connection Examples

#### Local PostgreSQL

```bash
DATABASE_URL="postgresql://postgres:password@localhost:5432/mydb"
```

#### Remote PostgreSQL

```bash
DATABASE_URL="postgresql://user:pass@remote-host:5432/production_db"
```

#### PostgreSQL with SSL

```bash
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require"
```

## Usage

### Running the MCP Server

#### From Cloned Project (Development)

```bash
# Using uv (recommended for development)
uv run python -m mcp_postgres

# Development mode with debug logging
uv run python -m mcp_postgres --dev

# With custom log level
uv run python -m mcp_postgres --log-level DEBUG
```

#### From Installed Package

```bash
# Using the installed script
mcp-postgres

# Or using Python module
python -m mcp_postgres

# Development mode
mcp-postgres --dev
```

### MCP Client Integration

#### Claude Desktop Configuration

Add to your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

##### For Cloned Project (Development)

```json
{
  "mcpServers": {
    "postgres": {
      "command": "C:/path/to/mcp-postgres/.venv/Scripts/python.exe",
      "args": ["-m", "mcp_postgres"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname"
      }
    }
  }
}
```

**Alternative using uv (if uv is in PATH):**

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uv",
      "args": ["run", "python", "-m", "mcp_postgres"],
      "cwd": "/path/to/mcp-postgres",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname"
      }
    }
  }
}
```

##### For Installed Package

```json
{
  "mcpServers": {
    "postgres": {
      "command": "mcp-postgres",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname"
      }
    }
  }
}
```

**Or using Python module:**

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["-m", "mcp_postgres"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname"
      }
    }
  }
}
```

#### Using with other MCP clients

```python
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    server_params = StdioServerParameters(
        command="python",
        args=["-m", "mcp_postgres"],
        env={"DATABASE_URL": "postgresql://user:pass@localhost:5432/db"}
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            # Initialize the connection
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print(f"Available tools: {len(tools.tools)}")

            # Execute a query
            result = await session.call_tool(
                "execute_query",
                arguments={
                    "query": "SELECT * FROM users WHERE age > $1",
                    "parameters": [25]
                }
            )
            print(result)

if __name__ == "__main__":
    asyncio.run(main())
```

## Available Tools

The server provides 50+ tools organized into 10 modules:

### Query Tools (3 tools)

- `execute_query`: Execute parameterized SQL queries safely
- `execute_raw_query`: Execute raw SQL with safety warnings
- `execute_transaction`: Execute multiple queries in a transaction

### Schema Tools (8 tools)

- `list_tables`: List all tables with metadata
- `describe_table`: Get detailed table structure
- `list_indexes`: Show table indexes and performance info
- `list_constraints`: Display foreign keys and constraints
- `list_views`: Show database views and definitions
- `list_functions`: List stored procedures and functions
- `list_triggers`: Display trigger definitions
- `list_sequences`: Show sequence information

### Analysis Tools (4 tools)

- `analyze_column`: Statistical analysis of column data
- `find_duplicates`: Detect duplicate records
- `profile_table`: Analyze data types and distributions
- `analyze_correlations`: Calculate column relationships

### Data Tools (4 tools)

- `insert_data`: Insert records with validation
- `update_data`: Update records with conditions
- `delete_data`: Delete records with safety checks
- `bulk_insert`: Efficient bulk data insertion

### Relation Tools (3 tools)

- `get_foreign_keys`: Map foreign key relationships
- `get_table_relationships`: Analyze table connections
- `validate_referential_integrity`: Check constraint violations

### Performance Tools (3 tools)

- `analyze_query_performance`: Analyze execution plans
- `find_slow_queries`: Identify performance bottlenecks
- `get_table_stats`: Get storage and access statistics

### Backup Tools (3 tools)

- `export_table_csv`: Export table data to CSV
- `import_csv_data`: Import CSV data with validation
- `backup_table`: Create complete table backups

### Admin Tools (4 tools)

- `get_database_info`: Get database version and info
- `monitor_connections`: Monitor active connections
- `vacuum_table`: Perform table maintenance
- `reindex_table`: Rebuild table indexes

### Validation Tools (3 tools)

- `validate_constraints`: Check constraint violations
- `validate_data_types`: Verify data type compliance
- `check_data_integrity`: Comprehensive integrity checks

### Generation Tools (3 tools)

- `generate_ddl`: Generate CREATE TABLE statements
- `generate_insert_template`: Create INSERT templates
- `generate_orm_model`: Generate ORM model classes

For detailed documentation of all MCP tools, please refer to the official documentation [TOOLS.md](TOOLS.md).

## Example Usage

### Basic Query Execution

```python
# Execute a parameterized query
result = await session.call_tool(
    "execute_query",
    arguments={
        "query": "SELECT name, email FROM users WHERE created_at > $1",
        "parameters": ["2024-01-01"]
    }
)
```

### Schema Inspection

```python
# List all tables
tables = await session.call_tool("list_tables")

# Get detailed table structure
table_info = await session.call_tool(
    "describe_table",
    arguments={"table_name": "users"}
)
```

### Data Analysis

```python
# Analyze column statistics
stats = await session.call_tool(
    "analyze_column",
    arguments={
        "table_name": "sales",
        "column_name": "amount"
    }
)

# Find duplicate records
duplicates = await session.call_tool(
    "find_duplicates",
    arguments={
        "table_name": "customers",
        "columns": ["email"]
    }
)
```

### Performance Analysis

```python
# Analyze query performance
performance = await session.call_tool(
    "analyze_query_performance",
    arguments={
        "query": "SELECT * FROM orders WHERE customer_id = 123"
    }
)

# Get table statistics
stats = await session.call_tool(
    "get_table_stats",
    arguments={"table_name": "orders"}
)
```

## Development

### Setup Development Environment

```bash
# Clone and setup
git clone <repository-url>
cd mcp-postgres
uv sync

# Install pre-commit hooks
uv run pre-commit install
```

### Code Quality

```bash
# Run linter and formatter
uv run ruff check --fix
uv run ruff format

# Type checking
uv run mypy src/

# Run all pre-commit hooks
uv run pre-commit run --all-files
```

### Testing

```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src/mcp_postgres

# Run specific test file
uv run pytest tests/unit/test_query_tools.py
```

### Project Structure

```text
src/mcp_postgres/
├── __init__.py
├── main.py                    # MCP server entry point
├── config/                    # Configuration management
├── core/                      # Core services (connection, security, context)
├── tools/                     # Tool modules (10 modules, 50+ tools)
└── utils/                     # Utilities (validators, formatters, exceptions)
```

## Security

- **SQL Injection Prevention**: All queries use parameter binding
- **Input Validation**: Comprehensive parameter validation
- **Access Control**: Table-level access validation
- **Error Handling**: Sanitized error messages without sensitive data
- **Connection Security**: Secure connection pool management

## Performance

- **Connection Pooling**: Async connection pool with configurable size
- **Query Optimization**: Execution plan analysis and slow query detection
- **Result Caching**: Schema metadata caching for improved performance
- **Bulk Operations**: Efficient bulk insert and export capabilities

## Troubleshooting

### Common Issues

#### Connection Issues

```bash
# Test database connection
psql "postgresql://user:pass@host:port/db" -c "SELECT version();"
```

#### Permission Issues

```bash
# Grant necessary permissions
GRANT CONNECT ON DATABASE mydb TO username;
GRANT USAGE ON SCHEMA public TO username;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO username;
```

#### Checking Environment Variables

```bash
# Check environment variables
echo $DATABASE_URL
```

### Logging

Set `LOG_LEVEL=DEBUG` for detailed logging:

```bash
LOG_LEVEL=DEBUG uv run python -m mcp_postgres
```

## Verification

After installation, verify that everything is working correctly:

### Test Installation

```bash
# Test module import
uv run python -c "import mcp_postgres; print('✓ Package installed correctly')"

# Test entry points
uv run python -m mcp_postgres --version
uv run mcp-postgres --version
```

### Test Development Tools

```bash
# Test linting and formatting
uv run ruff check src/
uv run ruff format src/

# Test type checking
uv run mypy src/

# Run pre-commit hooks
uv run pre-commit run --all-files
```

### Test MCP Server (requires database)

#### For Cloned Project

```bash
# Set up test database connection
export DATABASE_URL="postgresql://user:pass@localhost:5432/testdb"

# Test server startup (will exit after showing help)
uv run python -m mcp_postgres --help

# Test with actual database connection
uv run python -m mcp_postgres --dev

# Test module import
uv run python -c "import mcp_postgres; print('✓ Package installed correctly')"
```

#### For Installed Package (Production Installation)

```bash
# Set up test database connection
export DATABASE_URL="postgresql://user:pass@localhost:5432/testdb"

# Test server startup
mcp-postgres --help

# Test with actual database connection
mcp-postgres --dev

# Test module import
python -c "import mcp_postgres; print('✓ Package installed correctly')"
```

## Installation Troubleshooting

### Common Issues with Cloned Project

**Issue**: `ModuleNotFoundError: No module named 'mcp_postgres'`

```bash
# Solution: Install in development mode
uv pip install -e .
```

**Issue**: `uv: command not found`

```bash
# Solution: Install uv first
pip install uv
# Or follow installation guide: https://docs.astral.sh/uv/getting-started/installation/
```

**Issue**: Virtual environment not activated

```bash
# Solution: Use uv run or activate manually
uv run python -m mcp_postgres
# Or activate: source .venv/bin/activate (Linux/macOS) or .venv\Scripts\activate (Windows)
```

### Common Issues with Installed Package

**Issue**: `mcp-postgres: command not found`

```bash
# Solution: Use Python module instead
python -m mcp_postgres
```

**Issue**: Package not found during installation

```bash
# Solution: Install from local source
pip install -e /path/to/mcp-postgres
```

### Claude Desktop Configuration Issues

**Issue**: Server fails to start in Claude Desktop

- Check that the command path is correct and absolute
- Verify that the Python executable exists
- Ensure DATABASE_URL is properly formatted
- Check Claude Desktop logs for specific error messages

**Windows Path Example**:

```json
"command": "C:/Users/USERNAME/path/to/mcp-postgres/.venv/Scripts/python.exe"
```

**macOS/Linux Path Example**:

```json
"command": "/Users/username/path/to/mcp-postgres/.venv/bin/python"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests and linting
5. Submit a pull request

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.

## Support

For issues and questions:

- Create an issue on GitHub
- Check the troubleshooting section
- Review the MCP documentation
