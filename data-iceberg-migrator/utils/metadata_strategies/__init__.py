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
import math

logger = logging.getLogger(__name__)


def cell_str(val, default=''):
    """Safely convert a pandas cell value to a stripped string, handling NaN/None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return str(val).strip() or default


from utils.shared import normalize_s3  # noqa: F401 — re-exported for strategy modules


from .hive_to_hive import (  # noqa: E402
    create_dest_table as _hive_create,
    discover_tables as _hive_discover,
    parse_excel_rows as _hive_parse,
)

from .iceberg_to_iceberg import (  # noqa: E402
    create_dest_table as _iceberg_create,
    discover_tables as _iceberg_discover,
    parse_excel_rows as _iceberg_parse,
)

STRATEGIES = {
    'hive_to_hive': {
        'parse_excel_rows': _hive_parse,
        'discover_tables': _hive_discover,
        'create_dest_table': _hive_create,
    },
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
