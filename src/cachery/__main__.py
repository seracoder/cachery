"""``python -m cachery`` — delegate to the CLI."""

from cachery._cli.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
