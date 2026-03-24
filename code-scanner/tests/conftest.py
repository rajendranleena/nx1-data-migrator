"""
Shared pytest fixtures for code-scanner tests.
No external dependencies to mock — code-scanner.py is a standalone script.
"""

import sys
import textwrap
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Make code-scanner importable as a module
# ---------------------------------------------------------------------------
_MODULE_DIR = Path(__file__).resolve().parent.parent
_str_dir = str(_MODULE_DIR)
if _str_dir not in sys.path:
    sys.path.insert(0, _str_dir)

import importlib.util as _ilu  # noqa: E402


def _load_scanner_module():
    spec = _ilu.spec_from_file_location(
        "code_scanner",
        _MODULE_DIR / "code-scanner.py"
    )
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="session")
def cs():
    """Loaded code-scanner module, shared across the entire test session."""
    return _load_scanner_module()


# ---------------------------------------------------------------------------
# Temp-file helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_py(tmp_path):
    """
    Factory: write a .py snippet to a temp file and return its Path.
    Usage:  f = tmp_py('x = sqlContext.sql("SELECT 1")')
    """
    def _make(content: str, filename: str = "sample.py") -> Path:
        p = tmp_path / filename
        p.write_text(textwrap.dedent(content), encoding="utf-8")
        return p
    return _make


@pytest.fixture
def tmp_sql(tmp_path):
    """Factory: write a .sql snippet to a temp file and return its Path."""
    def _make(content: str, filename: str = "sample.sql") -> Path:
        p = tmp_path / filename
        p.write_text(textwrap.dedent(content), encoding="utf-8")
        return p
    return _make


@pytest.fixture
def tmp_dir(tmp_path):
    """Return a writable temporary directory Path."""
    return tmp_path


# ---------------------------------------------------------------------------
# Pre-built scanner instance
# ---------------------------------------------------------------------------

@pytest.fixture
def scanner(cs):
    """Fresh SparkMigrationScanner with all default rules."""
    return cs.SparkMigrationScanner()
