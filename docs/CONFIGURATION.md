# Configuration Guide

This document provides comprehensive configuration options for the MCP Postgres server.

## Environment Variables

### Database Connection (Required)

#### Option 1: Single DATABASE_URL

```bash
DATABASE_URL="postgresql://username:password@host:port/database"
```

#### Option 2: Individual Components

```bash
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_DATABASE="mydb"
POSTGRES_USERNAME="postgres"
POSTGRES_PASSWORD="password"
```

### Connection Pool Settings (Optional)

```bash
# Connection pool size (default: 10)
POSTGRES_POOL_SIZE="10"

# Maximum pool overflow (default: 20)
POSTGRES_MAX_OVERFLOW="20"

# Connection timeout in seconds (default: 30)
POSTGRES_CONNECT_TIMEOUT="30"

# Query timeout in seconds (default: 30)
QUERY_TIMEOUT="30"
```

### Server Settings (Optional)

```bash
# Logging level: DEBUG, INFO, WARNING, ERROR (default: INFO)
LOG_LEVEL="INFO"

# Enable development mode (default: false)
DEV_MODE="false"

# Server name for MCP identification (default: mcp-postgres)
SERVER_NAME="mcp-postgres"
```

### Security Settings (Optional)

```bash
# Enable query validation (default: true)
ENABLE_QUERY_VALIDATION="true"

# Maximum query result rows (default: 10000)
MAX_RESULT_ROWS="10000"

# Enable table access control (default: true)
ENABLE_TABLE_ACCESS_CONTROL="true"

# Allowed table patterns (comma-separated, default: all)
ALLOWED_TABLE_PATTERNS="public.*,app.*"

# Blocked table patterns (comma-separated)
BLOCKED_TABLE_PATTERNS="pg_*,information_schema.*"
```

## Configuration Files

### .env File Example

Create a `.env` file in your project root:

```bash
# Database connection
DATABASE_URL="postgresql://postgres:password@localhost:5432/myapp"

# Server settings
LOG_LEVEL="INFO"
QUERY_TIMEOUT="30"

# Connection pool
POSTGRES_POOL_SIZE="10"
POSTGRES_MAX_OVERFLOW="20"

# Security
MAX_RESULT_ROWS="5000"
ALLOWED_TABLE_PATTERNS="public.*,app.*"
```

### Python Configuration

You can also configure the server programmatically:

```python
import os
from mcp_postgres.config.settings import ServerConfig
from mcp_postgres.config.database import DatabaseConfig

# Override default configuration
os.environ.update({
    "DATABASE_URL": "postgresql://user:pass@localhost:5432/db",
    "LOG_LEVEL": "DEBUG",
    "POSTGRES_POOL_SIZE": "15"
})

# Or create config objects directly
db_config = DatabaseConfig(
    host="localhost",
    port=5432,
    database="myapp",
    username="postgres",
    password="password",
    pool_size=15,
    max_overflow=25
)
```

## Database Connection Examples

### Local Development

```bash
# Standard local PostgreSQL
DATABASE_URL="postgresql://postgres:password@localhost:5432/myapp_dev"

# Local with custom port
DATABASE_URL="postgresql://postgres:password@localhost:5433/myapp_dev"

# Local with Unix socket
DATABASE_URL="postgresql://postgres:password@/var/run/postgresql/myapp_dev"
```

### Production Environments

```bash
# Remote server with SSL
DATABASE_URL="postgresql://user:pass@db.example.com:5432/prod?sslmode=require"

# AWS RDS
DATABASE_URL="postgresql://username:password@mydb.abc123.us-east-1.rds.amazonaws.com:5432/production"

# Google Cloud SQL
DATABASE_URL="postgresql://user:pass@/cloudsql/project:region:instance/database"

# Azure Database
DATABASE_URL="postgresql://user@server:pass@server.postgres.database.azure.com:5432/database?sslmode=require"
```

### Connection with SSL Options

```bash
# Require SSL
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require"

# SSL with certificate verification
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=verify-full&sslcert=client.crt&sslkey=client.key&sslrootcert=ca.crt"

# SSL disabled (not recommended for production)
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=disable"
```

## MCP Client Configuration

### Claude Desktop

Add to your Claude Desktop configuration:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%/Claude/claude_desktop_config.json`

#### For Cloned Project (Development)

**Recommended approach (using virtual environment Python directly):**

```json
{
  "mcpServers": {
    "postgres": {
      "command": "C:/Users/USERNAME/path/to/mcp-postgres/.venv/Scripts/python.exe",
      "args": ["-m", "mcp_postgres"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "LOG_LEVEL": "INFO",
        "MAX_RESULT_ROWS": "1000"
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
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "LOG_LEVEL": "INFO",
        "MAX_RESULT_ROWS": "1000"
      }
    }
  }
}
```

#### For Installed Package

**Using the installed script:**

```json
{
  "mcpServers": {
    "postgres": {
      "command": "mcp-postgres",
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "LOG_LEVEL": "INFO",
        "MAX_RESULT_ROWS": "1000"
      }
    }
  }
}
```

**Using Python module:**

```json
{
  "mcpServers": {
    "postgres": {
      "command": "python",
      "args": ["-m", "mcp_postgres"],
      "env": {
        "DATABASE_URL": "postgresql://user:pass@localhost:5432/mydb",
        "LOG_LEVEL": "INFO",
        "MAX_RESULT_ROWS": "1000"
      }
    }
  }
}
```

### Development Configuration

#### For Cloned Project

**Using virtual environment Python:**

```json
{
  "mcpServers": {
    "postgres-dev": {
      "command": "C:/Users/USERNAME/path/to/mcp-postgres/.venv/Scripts/python.exe",
      "args": ["-m", "mcp_postgres", "--dev"],
      "env": {
        "DATABASE_URL": "postgresql://postgres:password@localhost:5432/dev_db",
        "LOG_LEVEL": "DEBUG"
      }
    }
  }
}
```

**Using uv:**

```json
{
  "mcpServers": {
    "postgres-dev": {
      "command": "uv",
      "args": ["run", "python", "-m", "mcp_postgres", "--dev"],
      "cwd": "/path/to/mcp-postgres",
      "env": {
        "DATABASE_URL": "postgresql://postgres:password@localhost:5432/dev_db",
        "LOG_LEVEL": "DEBUG"
      }
    }
  }
}
```

#### For Installed Package Development

```json
{
  "mcpServers": {
    "postgres-dev": {
      "command": "mcp-postgres",
      "args": ["--dev"],
      "env": {
        "DATABASE_URL": "postgresql://postgres:password@localhost:5432/dev_db",
        "LOG_LEVEL": "DEBUG"
      }
    }
  }
}
```

## Docker Configuration

### Docker Compose Example

```yaml
version: '3.8'
services:
  mcp-postgres:
    build: .
    environment:
      - DATABASE_URL=postgresql://postgres:password@db:5432/myapp
      - LOG_LEVEL=INFO
      - POSTGRES_POOL_SIZE=10
    depends_on:
      - db
    stdin_open: true
    tty: true

  db:
    image: postgres:15
    environment:
      - POSTGRES_DB=myapp
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

### Dockerfile

```dockerfile
FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy project files
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install dependencies
RUN uv sync --frozen

# Set environment variables
ENV PYTHONPATH=/app/src
ENV LOG_LEVEL=INFO

# Run the server
CMD ["uv", "run", "python", "-m", "mcp_postgres"]
```

## Configuration Validation

The server validates configuration on startup. Common validation errors:

### Database Connection Errors

```bash
# Missing DATABASE_URL
ERROR: DATABASE_URL environment variable is required

# Invalid connection string
ERROR: Invalid DATABASE_URL format

# Connection failed
ERROR: Failed to connect to database: connection refused
```

### Configuration Errors

```bash
# Invalid pool size
ERROR: POSTGRES_POOL_SIZE must be a positive integer

# Invalid log level
ERROR: LOG_LEVEL must be one of: DEBUG, INFO, WARNING, ERROR

# Invalid timeout
ERROR: QUERY_TIMEOUT must be a positive number
```

## Performance Tuning

### Connection Pool Sizing

```bash
# For high-concurrency applications
POSTGRES_POOL_SIZE="20"
POSTGRES_MAX_OVERFLOW="40"

# For low-concurrency applications
POSTGRES_POOL_SIZE="5"
POSTGRES_MAX_OVERFLOW="10"
```

### Query Optimization

```bash
# Limit result set size to prevent memory issues
MAX_RESULT_ROWS="1000"

# Reduce query timeout for faster failure detection
QUERY_TIMEOUT="15"
```

### Logging Configuration

```bash
# Production: minimal logging
LOG_LEVEL="WARNING"

# Development: detailed logging
LOG_LEVEL="DEBUG"

# Monitoring: info level with structured logs
LOG_LEVEL="INFO"
```

## Security Best Practices

### Database User Permissions

Create a dedicated database user with minimal permissions:

```sql
-- Create user
CREATE USER mcp_user WITH PASSWORD 'secure_password';

-- Grant database access
GRANT CONNECT ON DATABASE myapp TO mcp_user;

-- Grant schema usage
GRANT USAGE ON SCHEMA public TO mcp_user;

-- Grant table permissions (adjust as needed)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO mcp_user;

-- Grant sequence permissions for auto-increment columns
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO mcp_user;
```

### Environment Variable Security

```bash
# Use strong passwords
POSTGRES_PASSWORD="$(openssl rand -base64 32)"

# Restrict table access
ALLOWED_TABLE_PATTERNS="app_*,user_*"
BLOCKED_TABLE_PATTERNS="admin_*,system_*,pg_*"

# Enable query validation
ENABLE_QUERY_VALIDATION="true"
```

### SSL Configuration

```bash
# Always use SSL in production
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=require"

# For maximum security
DATABASE_URL="postgresql://user:pass@host:5432/db?sslmode=verify-full&sslcert=client.crt&sslkey=client.key&sslrootcert=ca.crt"
```

## Troubleshooting Configuration

### Debug Configuration Loading

```bash
# Enable debug logging to see configuration loading
LOG_LEVEL=DEBUG uv run python -m mcp_postgres
```

### Test Database Connection

```python
import asyncio
from mcp_postgres.core.connection import connection_manager

async def test_connection():
    await connection_manager.initialize()
    health = await connection_manager.health_check()
    print(f"Connection status: {health}")
    await connection_manager.close()

asyncio.run(test_connection())
```

### Validate Environment

```bash
# Check all environment variables
env | grep -E "(DATABASE|POSTGRES|LOG_LEVEL|QUERY_TIMEOUT)"

# Test specific configuration
python -c "from mcp_postgres.config.settings import validate_environment; validate_environment()"
```
