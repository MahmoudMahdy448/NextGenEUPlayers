"""Utility helpers for generating safe SQL identifiers.

This centralizes the sanitizer so both the DDL generator and the loader
use the exact same rules (lowercase, underscores, strip non-alnum,
length limits, collision-reserved prefixes).
"""
from typing import Any
import pandas as pd


def make_sql_ident(name: Any, max_len: int = 63) -> str:
    """Create a safe SQL identifier from an arbitrary name.

    - lowercases
    - replaces spaces with underscores
    - strips characters other than a-z0-9_
    - ensures it doesn't start with a digit
    - truncates to ``max_len`` (reserving a small suffix room)
    """
    nm = str(name or '').lower().replace(' ', '_')
    nm = pd.Series([nm]).str.replace(r'[^a-z0-9_]', '', regex=True).iloc[0]
    if nm == '' or nm[0].isdigit():
        nm = f'col_{nm}' if nm != '' else 'col'
    if len(nm) > max_len:
        nm = nm[: max_len - 4]
    return nm


def make_table_name(orig: Any) -> str:
    t = str(orig or '').lower()
    t = t.replace('-', '_')
    t = pd.Series([t]).str.replace(r'[^a-z0-9_]', '', regex=True).iloc[0]
    if t == '' or t[0].isdigit():
        t = f't_{t}' if t != '' else 't'
    if len(t) > 63:
        t = t[:63]
    return t


def normalize_name(s: Any) -> str:
    """Return a simplified alphanumeric lowercase form for fuzzy matching."""
    return ''.join([c.lower() for c in str(s or '') if c.isalnum()])
