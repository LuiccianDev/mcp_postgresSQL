"""Entry point for running mcp-postgres as a module."""

import asyncio
import sys

from .main import main as async_main


def main() -> None:
    """Sync wrapper for the async main function."""
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
