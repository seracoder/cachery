#!/usr/bin/env python3
"""
scripts/freshness_gate.py

Verify that every runtime dependency's GitHub repo has been pushed to
within the allowed window. Fails (non-zero exit) if any dependency is stale,
unresolvable, or archived.

Usage:
    python scripts/freshness_gate.py deps.toml --max-age-days 30
    GITHUB_TOKEN=ghp_xxx python scripts/freshness_gate.py deps.toml --max-age-days 30

deps.toml format:
    [deps]
    redis      = "redis/redis-py"
    msgspec    = "jcrist/msgspec"
    xxhash     = "ifduyue/python-xxhash"
    zstandard  = "indygreg/python-zstandard"
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import urllib.error
import urllib.request


def _load_config(path: str) -> dict[str, dict[str, str]]:
    """Parse the TOML config. ``tomllib`` is 3.11+; on 3.10 fall back to a
    minimal parser for the simple ``name = "owner/repo"`` format used here."""
    try:
        import tomllib
    except ModuleNotFoundError:
        deps: dict[str, str] = {}
        section: str | None = None
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.split("#", 1)[0].strip()
                if not line:
                    continue
                if line.startswith("["):
                    section = line.strip("[]").strip()
                    continue
                if section == "deps" and "=" in line:
                    name, _, value = line.partition("=")
                    deps[name.strip()] = value.strip().strip("'\"")
        return {"deps": deps}
    with open(path, "rb") as f:
        return tomllib.load(f)


def gh_get(repo: str, token: str | None) -> dict[str, object]:
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.load(resp)


def default_branch_last_commit(repo: str, branch: str, token: str | None) -> dt.datetime:
    """Stricter check: last commit on the default branch specifically,
    rather than `pushed_at` (which can reflect pushes to any branch/tag)."""
    req = urllib.request.Request(f"https://api.github.com/repos/{repo}/commits/{branch}")
    req.add_header("Accept", "application/vnd.github+json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.load(resp)
    date_str = data["commit"]["committer"]["date"]
    return dt.datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("config", help="TOML file with a [deps] table of name = 'owner/repo'")
    ap.add_argument("--max-age-days", type=int, default=30)
    ap.add_argument(
        "--strict-default-branch",
        action="store_true",
        help="Check last commit on the default branch instead of repo-wide pushed_at",
    )
    ap.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    args = ap.parse_args()

    cfg = _load_config(args.config)

    now = dt.datetime.now(dt.timezone.utc)
    failures: list[tuple[str, str, str]] = []

    for name, repo in cfg.get("deps", {}).items():
        try:
            meta = gh_get(repo, args.token)
        except urllib.error.HTTPError as e:
            failures.append((name, repo, f"HTTP {e.code}: {e.reason}"))
            continue
        except Exception as e:
            failures.append((name, repo, f"ERROR: {e}"))
            continue

        if meta.get("archived"):
            failures.append((name, repo, "REPO IS ARCHIVED"))
            continue

        try:
            if args.strict_default_branch:
                pushed = default_branch_last_commit(repo, str(meta["default_branch"]), args.token)
            else:
                pushed = dt.datetime.fromisoformat(str(meta["pushed_at"]).replace("Z", "+00:00"))
        except Exception as e:
            failures.append((name, repo, f"could not read commit date: {e}"))
            continue

        age_days = (now - pushed).days
        status = "OK   " if age_days <= args.max_age_days else "STALE"
        print(f"{status}  {name:<15} {repo:<35} last activity {age_days:>4}d ago ({pushed.date()})")
        if age_days > args.max_age_days:
            failures.append((name, repo, f"{age_days} days since last push (limit {args.max_age_days})"))

    if failures:
        print("\nFAILING dependencies:")
        for name, repo, reason in failures:
            print(f"  - {name} ({repo}): {reason}")
        print(f"\n{len(failures)} dependency(ies) failed the freshness gate.")
        sys.exit(1)

    print(
        f"\nAll {len(cfg.get('deps', {}))} dependencies within the {args.max_age_days}-day freshness window."
    )


if __name__ == "__main__":
    main()
