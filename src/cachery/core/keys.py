"""Cache-key building and *static* key-template validation.

The function signature is the source of truth for cache keys. Templates are
validated against ``inspect.signature`` **at decoration time** (i.e. at import),
so a typo like ``key="user:{used_id}"`` fails immediately with a
"did you mean 'user_id'?" error instead of silently cache-missing forever.

Template grammar:

- ``{param}`` — a parameter of the decorated function.
- ``{param.attr.sub}`` — dotted attribute traversal; the root must be a parameter.
- ``{param.0}`` / ``{args.0}`` — positional-vararg items; ``{kwargs.name}`` for
  var-keyword items.
- ``{@ctx_name}`` — a registered :class:`contextvars.ContextVar` (see
  :func:`register_context_var`).
- ``{param:hash(sha1)}`` — format specs: ``hash(<alg>)`` (also ``xxhash``),
  ``lower``, ``upper``.
- ``{param!r}`` / ``{param!s}`` conversions are supported.

When no template is given, a key is auto-generated from **all** bound parameters
in stable declaration order, honouring ``Annotated[..., KeyIgnore]`` and
``Annotated[..., KeyPart(name=..., fmt=...)]`` metadata.
"""

from __future__ import annotations

import dataclasses
import difflib
import hashlib
import inspect
import string
from contextvars import ContextVar
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from cachery.core.exceptions import CacheryError, InvalidKeyTemplateError

__all__ = ["KeyBuilder", "KeyIgnore", "KeyPart", "register_context_var"]


class KeyIgnore:
    """``Annotated[Request, KeyIgnore]`` — exclude this parameter from auto keys."""


@dataclasses.dataclass(frozen=True)
class KeyPart:
    """``Annotated[str, KeyPart(name="lang", fmt="lower")]`` — rename/reformat a
    parameter in auto-generated keys. ``fmt`` uses the template format-spec
    grammar (``lower``, ``upper``, ``hash(sha1)``, ...)."""

    name: str | None = None
    fmt: str | None = None


_CONTEXT_VARS: dict[str, ContextVar[Any]] = {}


def register_context_var(name: str, var: ContextVar[Any] | None = None) -> ContextVar[Any]:
    """Register a context variable usable in key templates as ``{@name}``."""
    if var is None:
        var = ContextVar(f"cachery:{name}", default=None)
    _CONTEXT_VARS[name] = var
    return var


_ADDR_RE = __import__("re").compile(r"0x[0-9a-fA-F]+")
_MAX_PLAIN_VALUE_LEN = 96


def _hash_str(value: str, alg: str) -> str:
    if alg == "xxhash":
        try:
            import xxhash  # noqa: PLC0415 - optional extra, lazy import

            digest: str = str(xxhash.xxh3_64_intdigest(value.encode()))
            return digest
        except ImportError as e:
            raise CacheryError(
                "Format spec 'hash(xxhash)' requires the 'speed' extra: pip install cachery[speed]"
            ) from e
    try:
        return hashlib.new(alg, value.encode(), usedforsecurity=False).hexdigest()
    except (ValueError, TypeError) as e:
        raise CacheryError(f"Unknown hash algorithm {alg!r} for key format spec") from e


def _format_value(value: Any, spec: str | None) -> str:
    """Convert a key-part value to its stable string form.

    dataclasses/attrs/pydantic objects have deterministic ``__repr__`` so
    ``repr`` is used directly; for anything else, default ``object.__repr__``
    memory addresses are scrubbed so keys stay stable across processes
    (the original library's silent footgun).
    """
    text: str
    if value is None or isinstance(value, (str, int, float, bool)):
        text = str(value)
    elif (
        (dataclasses.is_dataclass(value) and not isinstance(value, type))
        or hasattr(value, "__attrs_attrs__")
        or hasattr(value, "model_fields")
    ):
        text = repr(value)
    else:
        text = _ADDR_RE.sub("0xADDR", repr(value))
    if len(text) > _MAX_PLAIN_VALUE_LEN and not (spec and spec.startswith("hash(")):
        text = _hash_str(text, "blake2b")
    if not spec:
        return text
    if spec.startswith("hash(") and spec.endswith(")"):
        return _hash_str(text, spec[5:-1])
    if spec == "lower":
        return text.lower()
    if spec == "upper":
        return text.upper()
    raise CacheryError(f"Unsupported key format spec {spec!r}")


def _apply_conversion(value: Any, conversion: str | None) -> Any:
    if conversion == "r":
        return repr(value)
    if conversion == "s":
        return str(value)
    if conversion is None:
        return value
    raise CacheryError(f"Unsupported key template conversion {conversion!r}")


_SENTINEL = object()

_formatter = string.Formatter()


class KeyBuilder:
    """Validated-at-decoration-time key factory for one decorated function."""

    __slots__ = (
        "_defaults",
        "_func",
        "_hints",
        "_param_meta",
        "_params",
        "_positional_names",
        "_sig",
        "_template",
        "_template_fields",
    )

    def __init__(self, func: Any, template: str | None) -> None:
        self._func = func
        self._template = template
        try:
            sig = inspect.signature(func)
        except (TypeError, ValueError):
            sig = None
        self._sig = sig
        self._params: dict[str, inspect.Parameter] = dict(sig.parameters) if sig else {}
        self._hints: dict[str, Any] = {}
        try:
            self._hints = get_type_hints(func, include_extras=True)
        except Exception:
            self._hints = dict(getattr(func, "__annotations__", {}) or {})
        # Pre-parse the template once: (literal, field, spec, conversion) tuples.
        self._template_fields = list(_formatter.parse(template)) if template is not None else None
        # Pre-extract Annotated[...] key metadata once (was per-call).
        self._param_meta: dict[str, tuple[bool, KeyPart | None]] = {
            name: self._annotation_meta(name) for name in self._params
        }
        # Fast positional path: every declared parameter is positional-or-keyword
        # (no *args/**kwargs, no keyword-only) → direct arg→name mapping works.
        pos = [name for name, p in self._params.items() if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD]
        has_var = any(
            p.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            for p in self._params.values()
        )
        self._positional_names: list[str] | None = (
            pos if pos and not has_var and len(pos) == len(self._params) else None
        )
        self._defaults: dict[str, Any] = {
            name: p.default for name, p in self._params.items() if p.default is not inspect.Parameter.empty
        }
        if template is not None:
            self._validate(template)

    # -- validation ---------------------------------------------------------

    def _allowed_roots(self) -> list[str]:
        allowed = [
            name
            for name, p in self._params.items()
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        ]
        if any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in self._params.values()):
            allowed.append("args")
        if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in self._params.values()):
            allowed.append("kwargs")
        return allowed

    def _validate(self, template: str) -> None:
        allowed = self._allowed_roots()
        for _literal, field_name, _spec, conversion in _formatter.parse(template):
            if field_name is None:
                continue
            if field_name == "":
                raise self._error(template, "empty placeholder '{}'", [])
            root = field_name.split(".", 1)[0]
            if root.startswith("@") or root in allowed:
                if conversion not in (None, "r", "s"):
                    raise self._error(template, f"unsupported conversion {conversion!r}", allowed)
                continue
            raise self._error(template, f"unknown placeholder {field_name!r}", allowed)

    def _error(self, template: str, problem: str, allowed: list[str]) -> InvalidKeyTemplateError:
        suggestions = difflib.get_close_matches(problem.split("'")[1] if "'" in problem else "", allowed)
        did_you_mean = f" — did you mean '{suggestions[0]}'?" if suggestions else ""
        where = ""
        code = getattr(self._func, "__code__", None)
        if code is not None:
            file = getattr(code, "co_filename", "?")
            where = f" at {file}:{code.co_firstlineno}"
        params = ", ".join(allowed) or "<none>"
        return InvalidKeyTemplateError(
            f"{problem.capitalize()} in key template {template!r} for "
            f"{getattr(self._func, '__qualname__', self._func)!r}{where}{did_you_mean}\n"
            f"Available parameters: {params}"
        )

    # -- key building -------------------------------------------------------

    def build(self, args: tuple[Any, ...], kwargs: dict[str, Any], prefix: str = "") -> str:
        """Build the cache key for one call.

        Hot-path optimisation: when the call supplies exactly the declared
        positional parameters (no kwargs, no defaults to fill), the mapping is
        assembled directly instead of going through ``inspect`` binding — the
        dominant cost for small signatures.
        """
        qualname = getattr(self._func, "__qualname__", "func")
        if not self._params:
            # Un-introspectable callable: fall back to arg reprs.
            body = ":".join(_format_value(a, None) for a in (*args, *kwargs.values()))
            return f"{prefix}{qualname}:{body}" if body else f"{prefix}{qualname}"
        arguments = self._bind_arguments(args, kwargs)
        parts = [prefix + f"{self._func.__module__}.{qualname}"]
        if self._template is not None:
            parts.append(self._from_template(arguments))
        else:
            parts.extend(self._auto_parts(arguments))
        return ":".join(p for p in parts if p)

    def _bind_arguments(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        """Fast positional path, falling back to ``inspect`` binding."""
        names = self._positional_names
        if names is not None and len(args) <= len(names):
            fast = (
                self._fast_bind_no_kwargs(args, names)
                if not kwargs
                else self._fast_bind_with_kwargs(args, kwargs, names)
            )
            if fast is not None:
                return fast
        sig = self._sig
        if sig is None:  # pragma: no cover - _params non-empty implies sig exists
            raise CacheryError("Cannot build cache key: signature unavailable")
        try:
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
        except TypeError as e:
            raise CacheryError(f"Cannot build cache key: argument binding failed: {e}") from e
        return dict(bound.arguments)

    def _fast_bind_no_kwargs(self, args: tuple[Any, ...], names: list[str]) -> dict[str, Any] | None:
        """All-positional fast path, or ``None`` when it can't apply."""
        if len(args) == len(names):
            return dict(zip(names, args, strict=True))
        # Positional prefix only → fill the rest from precomputed defaults.
        arguments = dict(zip(names[: len(args)], args, strict=True))
        defaults = self._defaults
        for name in names[len(args) :]:
            if name in defaults:
                arguments[name] = defaults[name]
            else:
                return None  # no default → genuine TypeError below
        return arguments

    def _fast_bind_with_kwargs(
        self, args: tuple[Any, ...], kwargs: dict[str, Any], names: list[str]
    ) -> dict[str, Any] | None:
        """Positional-prefix + keyword fast path, or ``None`` when it can't apply."""
        if len(args) >= len(names):
            return None
        arguments = dict(zip(names[: len(args)], args, strict=True))
        remaining = names[len(args) :]
        if not all(k in remaining for k in kwargs):
            return None
        arguments.update(kwargs)
        defaults = self._defaults
        for name in remaining:
            if name not in arguments:
                if name in defaults:
                    arguments[name] = defaults[name]
                else:
                    return None
        return arguments

    # -- explicit template --------------------------------------------------

    def render(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        """Render the explicit template only (used for tag templates)."""
        if self._template is None:
            raise CacheryError("render() requires an explicit template")
        try:
            arguments = self._bind_arguments(args, kwargs)
        except CacheryError as e:
            raise CacheryError(f"Cannot render template: {e}") from e
        return self._from_template(arguments)

    def _resolve_field(self, arguments: dict[str, Any], field_name: str) -> Any:
        root, *path = field_name.split(".")
        if root.startswith("@"):
            name = root[1:]
            var = _CONTEXT_VARS.get(name)
            if var is None:
                raise CacheryError(
                    f"Context variable {name!r} used in key template but not registered; "
                    f"call register_context_var({name!r}) first."
                )
            current: Any = var.get()
        else:
            if root not in arguments:
                raise CacheryError(f"Parameter {root!r} missing when building cache key")
            current = arguments[root]
        for part in path:
            if isinstance(current, dict):
                current = current.get(part, _SENTINEL)
                if current is _SENTINEL:
                    raise CacheryError(f"Key {part!r} missing in mapping while building cache key")
            elif part.isdigit() and isinstance(current, (list, tuple)):
                current = current[int(part)]
            else:
                current = getattr(current, part, _SENTINEL)
                if current is _SENTINEL:
                    raise CacheryError(f"Attribute {part!r} missing while building cache key")
        return current

    def _from_template(self, arguments: dict[str, Any]) -> str:
        out: list[str] = []
        for literal, field_name, spec, conversion in self._template_fields or ():
            out.append(literal)
            if field_name is None:
                continue
            value = _apply_conversion(self._resolve_field(arguments, field_name), conversion)
            out.append(_format_value(value, spec))
        return "".join(out)

    # -- auto-generated key ---------------------------------------------------

    def _auto_parts(self, arguments: dict[str, Any]) -> list[str]:
        parts: list[str] = []
        first = next(iter(self._params), None)
        for name, param in self._params.items():
            if name == first and name in ("self", "cls"):
                # Instance identity belongs in the key when instances hold
                # per-tenant state. Deterministic repr (dataclass/attrs/
                # pydantic) when available; plain objects fall back to
                # id() — distinct per instance within this process. Without
                # this, two instances would share one cache entry.
                if name == "self" and "self" in arguments:
                    instance = arguments["self"]
                    if (
                        dataclasses.is_dataclass(instance)
                        or hasattr(instance, "__attrs_attrs__")
                        or hasattr(instance, "model_fields")
                    ):
                        parts.append(f"self:{instance!r}")
                    else:
                        parts.append(f"self:id={id(instance)}")
                continue
            if param.kind is inspect.Parameter.VAR_POSITIONAL:
                for i, item in enumerate(arguments.get(name, ())):
                    parts.append(f"args.{i}:{_format_value(item, None)}")
            elif param.kind is inspect.Parameter.VAR_KEYWORD:
                for key in sorted(arguments.get(name, {})):
                    parts.append(f"kwargs.{key}:{_format_value(arguments[name][key], None)}")
            else:
                meta_ignore, meta_part = self._param_meta.get(name, (False, None))
                if meta_ignore:
                    continue
                spec = meta_part.fmt if meta_part else None
                key_name = meta_part.name if meta_part and meta_part.name else name
                parts.append(f"{key_name}:{_format_value(arguments.get(name), spec)}")
        return parts

    def _annotation_meta(self, param: str) -> tuple[bool, KeyPart | None]:
        hint = self._hints.get(param)
        if get_origin(hint) is not Annotated:
            return False, None
        _typ, *metadata = get_args(hint)
        ignore = any(m is KeyIgnore for m in metadata)
        part = next((m for m in metadata if isinstance(m, KeyPart)), None)
        return ignore, part
