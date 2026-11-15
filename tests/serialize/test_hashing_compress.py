"""Hashing and compression helper tests."""

from __future__ import annotations

import pytest

from cachery.serialize import compress, hashing


def test_digest_blake2b_default() -> None:
    d1 = hashing.digest(b"data")
    d2 = hashing.digest(b"data")
    assert d1 == d2 and len(d1) == 128  # blake2b hex digest


def test_digest_xxhash_optional() -> None:
    xxhash = pytest.importorskip("xxhash")
    d = hashing.digest(b"data", alg="xxhash")
    assert d == xxhash.xxh3_64_hexdigest(b"data")


def test_digest_stable_across_optional_libs() -> None:
    """Same algorithm must produce identical output with/without extras."""
    import hashlib

    assert hashing.digest(b"x", alg="sha256") == hashlib.sha256(b"x").hexdigest()


def test_hmac_digest_keyed() -> None:
    a = hashing.hmac_digest(b"secret", b"data")
    b = hashing.hmac_digest(b"other", b"data")
    assert a != b
    assert hashing.hmac_digest(b"secret", b"data") == a


def test_hmac_key_longer_than_block_is_accepted() -> None:
    long_key = b"k" * 200
    assert hashing.hmac_digest(long_key, b"data") == hashing.hmac_digest(long_key, b"data")


def test_compress_zlib_roundtrip() -> None:
    data = b"payload" * 100
    assert compress.decompress(compress.compress(data)) == data


def test_compress_zstd_roundtrip() -> None:
    pytest.importorskip("zstandard")
    data = b"payload" * 100
    assert compress.decompress(compress.compress(data, "zstd"), "zstd") == data


def test_compress_unknown_method() -> None:
    with pytest.raises(ValueError, match="method"):
        compress.compress(b"x", "gzip")
    with pytest.raises(ValueError, match="method"):
        compress.decompress(b"x", "gzip")
