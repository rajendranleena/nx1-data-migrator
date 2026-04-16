"""
Metadata migration strategy registry.

Each strategy provides three functions:
  - parse_excel_rows(df, config, run_id) -> list[dict]
  - discover_tables(db_config, spark, config) -> list[dict]
  - create_dest_table(table_info, dest_db, spark, config) -> dict

Adding a new migration type:
  1. Create a new module in this package (e.g. my_type.py)
  2. Implement the three functions above
  3. Import and register them in STRATEGIES below
"""

import logging

from utils.migrations.shared import cell_str, normalize_s3  # noqa: F401 — re-exported for strategy modules

from .iceberg_to_iceberg import create_dest_table as _iceberg_create
from .iceberg_to_iceberg import discover_tables as _iceberg_discover
from .iceberg_to_iceberg import parse_excel_rows as _iceberg_parse

logger = logging.getLogger(__name__)

STRATEGIES = {
    'iceberg_to_iceberg': {
        'parse_excel_rows': _iceberg_parse,
        'discover_tables': _iceberg_discover,
        'create_dest_table': _iceberg_create,
    },
}


def get_strategy(migration_type: str) -> dict:
    """Look up a strategy by name. Raises ValueError for unknown types."""
    if migration_type not in STRATEGIES:
        raise ValueError(
            f"Unknown migration_type '{migration_type}'. "
            f"Available: {', '.join(STRATEGIES)}"
        )
    return STRATEGIES[migration_type]
