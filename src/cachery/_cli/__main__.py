"""``python -m cachery`` — production debugging CLI.

Stdlib ``argparse`` only. Commands: ``ping``, ``keys``, ``stats``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import Any

from cachery.backends.registry import create_backend


def _build(url: str) -> Any:
    return create_backend(url)


async def _ping(url: str) -> int:
    backend = None
    try:
        backend = _build(url)
        pong = await asyncio.wait_for(backend.ping(), timeout=3)
        print(f"OK {url} -> {pong.decode(errors='replace')}")
        return 0
    except Exception as e:
        print(f"FAIL {url}: {e}")
        return 1
    finally:
        if backend is not None:
            await backend.close()


async def _keys(url: str, pattern: str, limit: int) -> int:
    backend = None
    try:
        backend = _build(url)
        count = 0
        async for key in backend.scan(pattern):
            print(key)
            count += 1
            if count >= limit:
                print(f"... (limit {limit} reached)", file=sys.stderr)
                break
        return 0
    except Exception as e:
        print(f"FAIL {url}: {e}", file=sys.stderr)
        return 1
    finally:
        if backend is not None:
            await backend.close()


async def _stats(url: str, pattern: str) -> int:
    backend = None
    try:
        backend = _build(url)
        keys = [k async for k in backend.scan(pattern)]
        size = 0
        for key in keys[:1000]:
            value = await backend.get(key)
            if value is not None:
                size += len(value) if isinstance(value, (bytes, str)) else 1
        print(f"url:     {url}")
        print(f"pattern: {pattern}")
        print(f"keys:    {len(keys)}")
        print(f"bytes:   ~{size} (first {min(len(keys), 1000)} keys)")
        return 0
    except Exception as e:
        print(f"FAIL {url}: {e}", file=sys.stderr)
        return 1
    finally:
        if backend is not None:
            await backend.close()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m cachery",
        description="Inspect a cachery cache from the command line.",
    )
    ap.add_argument("--url", default="mem://", help="Backend URL (default mem://)")
    sub = ap.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="Check the backend is reachable")

    p_keys = sub.add_parser("keys", help="List keys matching a pattern")
    p_keys.add_argument("pattern", nargs="?", default="*")
    p_keys.add_argument("--limit", type=int, default=100)

    p_stats = sub.add_parser("stats", help="Key count and approximate size")
    p_stats.add_argument("pattern", nargs="?", default="*")

    args = ap.parse_args(argv)
    if args.command == "ping":
        return asyncio.run(_ping(args.url))
    if args.command == "keys":
        return asyncio.run(_keys(args.url, args.pattern, args.limit))
    return asyncio.run(_stats(args.url, args.pattern))


if __name__ == "__main__":
    sys.exit(main())
