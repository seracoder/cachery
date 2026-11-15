# Security Policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | ✅ |

## Reporting a vulnerability

Please report security issues privately via
[GitHub Security Advisories](https://github.com/seracoder/cachery/security/advisories/new)
("Report a vulnerability"). Do **not** open a public issue for anything you
believe is exploitable.

You will get an initial response within 7 days. Please include a description,
reproduction steps, affected versions, and any mitigations you have identified.

## Security-relevant design notes

- **cachery never pickles raw user input from untrusted sources.** Values you
  put in the cache are serialized with a tag-dispatch codec; `pickle` is only
  the fallback for types the safer paths cannot represent. Only cache data you
  control — a cache is not a trust boundary.
- **Cache-key privacy**: with `cache.setup(..., secret=...)` cache keys are
  keyed-hashed (blake2b by default) so key material (user IDs, tenant names)
  is not readable from a shared backend. Note this is obfuscation, not
  encryption — the backend can still see values.
- **Shared backends**: anyone who can read your Redis/SQLite can read cached
  values. Do not point cachery at a backend shared with untrusted parties, and
  do not cache secrets.
- **`python -m cachery`** is a read-only inspection tool for the configured
  backend; it never mutates values (beyond what a `ping`/`scan` requires).
