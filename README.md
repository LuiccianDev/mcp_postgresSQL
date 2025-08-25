<div align="center">
  <h1>MCP PostgreSQL</h1>

  <p>
    <em>A comprehensive Model Context Protocol (MCP) server for PostgreSQL database interactions.</em>
  </p>

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![MCP Compatible](https://img.shields.io/badge/MCP-Compatible-brightgreen)](https://modelcontextprotocol.io)

</div>

## Installation

### Prerequisites

- **Python** 3.13 or higher
- **PostgreSQL database** (local or remote)
- **UV Package Manager**: [Install UV](https://docs.astral.sh/uv/getting-started/installation/) (recommended) or use pip
- **Git**: For cloning the repository
- **Desktop Extensions (DXT)**: for creating .dxt packages for Claude desktop [Install DXT](https://github.com/anthropics/dxt)

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
      "command": "/path/to/repo/mcp-postgres/.venv/Scripts/python.exe",
      "args": ["-m", "mcp_postgres"],
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
      "command": "uv",
      "args": ["run", "mcp_postgres"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/dbname"
      }
    }
  }
}
```

#### DXT Package Deployment

**Best for**: Integrated DXT ecosystem users who want seamless configuration management.

1. **Package the project**:

   ```bash
   dxt pack
   ```

2. **Configuration**: The DXT package automatically handles dependencies and provides user-friendly configuration through the manifest.json:
   - `MCP_ALLOWED_DIRECTORIES`: Base directory for file operations

3. **Usage**: Once packaged, the tool integrates directly with DXT-compatible clients with automatic user configuration variable substitution.

4. **Server Configuration**: This project includes the [manifest.json](manifest.json) file for building the .dxt package.

For more details see [DXT Package Documentation](https://github.com/anthropics/dxt).

## Available Tools

The server provides tools organized into 10 modules:

For detailed documentation of all MCP tools, please refer to the official documentation [TOOLS.md](TOOLS.md).

## Development

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

---

## Project Structure

The following structure facilitates scalability and maintainability. Each folder and file has a clear responsibility within the project.

```text
c:\Users\DAVID\Desktop\mcp_server_local\mcp-postgres\src\mcp_postgres\
│
├── __init__.py         # MCP Postgres package initializer
├── main.py             # MCP server entry point
│
├── config\             # Configuration, environment variables, and secrets
│   ├── env.py          # Loads and validates environment variables
│   └── settings.py     # General server configuration
│
├── core\               # Core services: DB connection, security, context
│   ├── db.py           # PostgreSQL pool connection and management
│   ├── security.py     # Access validation and data protection
│   └── context.py      # User and session context management
│
├── tools\              # MCP tool modules (10 modules, 50+ tools)
│   ├── query_tools.py  # SQL query tools
│   ├── schema_tools.py # Schema management tools
│   └── ...             # Other tool modules
│
└── utils\              # General utilities: validators, formatters, exceptions
    ├── validators.py   # Parameter and data validation
    ├── formatters.py   # Result and error formatting
    └── exceptions.py   # Custom exception handling
```

| Absolute Path                                                         | Description                                                        |
|-----------------------------------------------------------------------|--------------------------------------------------------------------|
| `src\mcp_postgres\main.py`                                            | Main entry point for the MCP server                                |
| `src\mcp_postgres\config\`                                            | Configuration, environment variables, and secrets management       |
| `src\mcp_postgres\core\`                                              | Core services: DB connection, security, user context               |
| `src\mcp_postgres\tools\`                                             | MCP tool modules for database operations                           |
| `src\mcp_postgres\utils\`                                             | General utilities and helpers for validation and error handling     |

> **Tip:** Explore each folder to understand the responsibility of each module and make future contributions easier.

---

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

## Contribution

Contributions are welcome. Please read the contribution guidelines before submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

<div align="center">
  <p><strong>MCP Postgres Server</strong></p>
  <p>Empowering AI assistants with comprehensive Postgres database capabilities</p>
  <p>
    <a href="https://github.com/LuiccianDev/mcp_postgreSQL">🏠 GitHub</a> •
    <a href="https://modelcontextprotocol.io">🔗 MCP Protocol</a> •
    <a href="https://github.com/LuiccianDev/mcp_postgreSQL/blob/main/TOOLS.md">📚 Tool Documentation</a>
  </p>
  <p><em>Created with by LuiccianDev</em></p>
</div>
