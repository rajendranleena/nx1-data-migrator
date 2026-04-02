"""
Iceberg-to-Iceberg metadata migration strategy.

Source: File-based Iceberg catalog on S3 (no metastore — metadata.json files)
Destination: Iceberg tables registered in Hive Metastore via register_table
"""

import json
import logging

from utils.metadata_strategies import cell_str, normalize_s3
from utils.shared import compute_dest_path

logger = logging.getLogger(__name__)

ICEBERG_TYPE_MAP = {
    'boolean': 'BOOLEAN',
    'int': 'INT',
    'long': 'BIGINT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'date': 'DATE',
    'time': 'STRING',
    'timestamp': 'TIMESTAMP',
    'timestamptz': 'TIMESTAMP',
    'string': 'STRING',
    'binary': 'BINARY',
    'uuid': 'STRING',
}


def _map_iceberg_type(iceberg_type):
    """Map an Iceberg type to a Spark SQL type string."""
    if isinstance(iceberg_type, dict):
        return 'STRING'

    t = str(iceberg_type).lower()
    if t in ICEBERG_TYPE_MAP:
        return ICEBERG_TYPE_MAP[t]
    if t.startswith('decimal'):
        return t.upper()
    if t.startswith('fixed'):
        return 'BINARY'
    return 'STRING'


def _read_iceberg_metadata(spark, table_path):
    """Read and parse the latest Iceberg metadata.json from S3."""
    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.*')

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(table_path),
        spark._jsc.hadoopConfiguration()
    )

    metadata_dir = f"{table_path}/metadata"
    hint_path = spark._jvm.org.apache.hadoop.fs.Path(f"{metadata_dir}/version-hint.text")

    if fs.exists(hint_path):
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(hint_path))
        )
        version = reader.readLine().strip()
        reader.close()
        metadata_file = f"{metadata_dir}/v{version}.metadata.json"
    else:
        status_list = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(metadata_dir))
        metadata_files = []
        for i in range(len(status_list)):
            name = status_list[i].getPath().getName()
            if name.endswith('.metadata.json'):
                metadata_files.append(status_list[i].getPath().toString())
        if not metadata_files:
            raise FileNotFoundError(f"No metadata.json files found in {metadata_dir}")
        metadata_file = sorted(metadata_files)[-1]

    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(
            fs.open(spark._jvm.org.apache.hadoop.fs.Path(metadata_file)), "UTF-8"
        )
    )
    try:
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
    finally:
        reader.close()

    return json.loads('\n'.join(lines))


def _extract_schema(metadata):
    """Extract schema from Iceberg metadata as list of {name, type} dicts."""
    current_schema_id = metadata.get('current-schema-id', 0)
    schemas = metadata.get('schemas', [])

    schema = None
    for s in schemas:
        if s.get('schema-id') == current_schema_id:
            schema = s
            break
    if schema is None and schemas:
        schema = schemas[-1]
    if schema is None:
        return []

    return [
        {'name': field['name'], 'type': _map_iceberg_type(field['type'])}
        for field in schema.get('fields', [])
    ]


def _extract_partition_spec(metadata):
    """Extract partition columns from Iceberg metadata. Returns (col_names, is_partitioned)."""
    default_spec_id = metadata.get('default-spec-id', 0)
    specs = metadata.get('partition-specs', [])

    spec = None
    for s in specs:
        if s.get('spec-id') == default_spec_id:
            spec = s
            break
    if spec is None and specs:
        spec = specs[-1]

    if not spec or not spec.get('fields'):
        return [], False

    current_schema_id = metadata.get('current-schema-id', 0)
    schemas = metadata.get('schemas', [])
    schema = None
    for s in schemas:
        if s.get('schema-id') == current_schema_id:
            schema = s
            break
    if schema is None and schemas:
        schema = schemas[-1]

    field_id_to_name = {}
    if schema:
        for f in schema.get('fields', []):
            field_id_to_name[f['id']] = f['name']

    part_cols = []
    for pf in spec.get('fields', []):
        source_id = pf.get('source-id')
        name = field_id_to_name.get(source_id, f'field_{source_id}')
        part_cols.append(name)

    return part_cols, len(part_cols) > 0


def _extract_row_count(metadata):
    """Extract total row count from the current snapshot summary."""
    current_snapshot_id = metadata.get('current-snapshot-id')
    if current_snapshot_id is None:
        return 0

    for snap in metadata.get('snapshots', []):
        if snap.get('snapshot-id') == current_snapshot_id:
            summary = snap.get('summary', {})
            return int(summary.get('total-records', 0))

    return 0


def _get_fs_stats(spark, table_path):
    """Get file count and total size via Hadoop FS."""
    try:
        from py4j.java_gateway import java_import

        java_import(spark._jvm, 'org.apache.hadoop.fs.*')
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(table_path),
            spark._jsc.hadoopConfiguration()
        )
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(table_path)
        if fs.exists(path_obj):
            summary = fs.getContentSummary(path_obj)
            return int(summary.getFileCount()), int(summary.getLength())
    except Exception as e:
        logger.warning(f"[iceberg_discover] Could not get FS stats for {table_path}: {e}")
    return 0, 0


# ─────────────────────────────────────────────────────────────────────────────
# Strategy interface functions
# ─────────────────────────────────────────────────────────────────────────────

def parse_excel_rows(df, config, run_id):
    """Parse Excel rows for Iceberg-to-Iceberg metadata migration.

    Each row specifies a single Iceberg table via source_table_path.
    Rows are grouped by (src_db, dest_db, dest_bucket, prefixes).
    """
    grouped = {}
    for _, row in df.iterrows():
        src_db = cell_str(row.get('database'))
        if not src_db:
            continue

        table_name = cell_str(row.get('table'))
        source_table_path = normalize_s3(cell_str(row.get('source_table_path')))

        if not table_name or not source_table_path:
            logger.warning(
                f"[parse_iceberg_excel] Skipping row for database '{src_db}' — "
                f"missing 'table' or 'source_table_path'"
            )
            continue

        dest_db = cell_str(row.get('dest_database'), src_db)
        raw_bucket = cell_str(row.get('dest_bucket'))
        src_prefix = normalize_s3(cell_str(row.get('source_s3_prefix')))
        dest_prefix = normalize_s3(cell_str(row.get('dest_s3_prefix')))
        dest_bucket_norm = normalize_s3(raw_bucket) if raw_bucket else config['default_s3_bucket']

        key = (src_db, dest_db, dest_bucket_norm, src_prefix, dest_prefix)
        if key not in grouped:
            grouped[key] = {'entries': []}

        grouped[key]['entries'].append({
            'table_name': table_name,
            'source_table_path': source_table_path.rstrip('/'),
        })

    configs = []
    for (src_db, dest_db, dest_bucket, src_prefix, dest_prefix), group in grouped.items():
        configs.append({
            'source_database': src_db,
            'dest_database': dest_db,
            'dest_bucket': dest_bucket,
            'source_s3_prefix': src_prefix,
            'dest_s3_prefix': dest_prefix,
            'table_entries': group['entries'],
            'run_id': run_id,
        })
        logger.info(
            f"[parse_iceberg_excel] {src_db} → {dest_db} | bucket={dest_bucket} | "
            f"tables={len(group['entries'])}"
        )

    return configs


def discover_tables(db_config, spark, config):
    """Discover Iceberg table metadata by reading metadata.json from S3."""
    src_db = db_config['source_database']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    src_prefix = db_config.get('source_s3_prefix', '')
    dest_prefix = db_config.get('dest_s3_prefix', '')
    entries = db_config.get('table_entries', [])

    logger.info(f"[iceberg_discover] '{src_db}': discovering {len(entries)} table(s)")

    metadata_list = []
    for entry in entries:
        tbl_name = entry['table_name']
        table_path = entry['source_table_path'].rstrip('/')

        try:
            iceberg_meta = _read_iceberg_metadata(spark, table_path)
            schema = _extract_schema(iceberg_meta)
            part_cols, is_partitioned = _extract_partition_spec(iceberg_meta)
            row_count = _extract_row_count(iceberg_meta)
            file_format = iceberg_meta.get('properties', {}).get(
                'write.format.default', 'parquet'
            ).upper()
            file_count, total_size = _get_fs_stats(spark, table_path)

            if src_prefix and dest_prefix:
                dest_path = compute_dest_path(
                    source_location=table_path,
                    dest_database=dest_db,
                    table_name=tbl_name,
                    dest_bucket=dest_bucket,
                    source_s3_prefix=src_prefix,
                    dest_s3_prefix=dest_prefix,
                )
            else:
                dest_path = table_path

            logger.info(
                f"[iceberg_discover] {src_db}.{tbl_name} | fmt={file_format} | "
                f"rows={row_count} | size={total_size / (1024 ** 2):.1f}MB | dest={dest_path}"
            )

            metadata_list.append({
                'source_database': src_db,
                'source_table': tbl_name,
                'dest_database': dest_db,
                'dest_bucket': dest_bucket,
                'source_location': table_path,
                'dest_location': dest_path,
                'file_format': file_format,
                'table_type': 'ICEBERG',
                'schema': schema,
                'partition_columns': ','.join(part_cols),
                'partitions': [],
                'partition_count': 0,
                'is_partitioned': is_partitioned,
                'source_row_count': row_count,
                'source_file_count': file_count,
                'source_total_size_bytes': total_size,
            })

        except Exception as e:
            logger.error(f"[iceberg_discover] FAILED for {src_db}.{tbl_name}: {e}")
            metadata_list.append({
                'source_database': src_db,
                'source_table': tbl_name,
                'dest_database': dest_db,
                'dest_bucket': dest_bucket,
                'source_location': table_path,
                'dest_location': '',
                'file_format': 'UNKNOWN',
                'table_type': 'UNKNOWN',
                'schema': [],
                'partition_columns': '',
                'partitions': [],
                'partition_count': 0,
                'is_partitioned': False,
                'source_row_count': 0,
                'source_file_count': 0,
                'source_total_size_bytes': 0,
                'error': str(e)[:500],
            })

    return metadata_list


def create_dest_table(table_info, dest_db, spark, config):
    """Register an Iceberg table in Hive Metastore via register_table procedure."""
    from utils.shared import apply_bucket_credentials

    tbl = table_info['source_table']
    dest_path = table_info['dest_location']
    full_name = f"{dest_db}.{tbl}"

    apply_bucket_credentials(
        spark, dest_path,
        config.get('_dest_endpoint', ''),
        config.get('_dest_access_key', ''),
        config.get('_dest_secret_key', ''),
    )

    try:
        exists = False
        try:
            spark.sql(f"DESCRIBE {full_name}")
            exists = True
        except Exception:
            pass

        if exists:
            spark.sql(f"REFRESH TABLE {full_name}")
            logger.info(f"[iceberg_create] REFRESHED (already existed): {full_name}")
            return {'source_table': tbl, 'status': 'COMPLETED', 'existed': True, 'error': None}

        spark.sql(
            f"CALL spark_catalog.system.register_table('{full_name}', '{dest_path}')"
        )
        logger.info(f"[iceberg_create] REGISTERED: {full_name} at {dest_path}")
        return {'source_table': tbl, 'status': 'COMPLETED', 'existed': False, 'error': None}

    except Exception as e:
        error_msg = str(e)[:2000]
        logger.error(f"[iceberg_create] FAILED for {full_name}: {error_msg}")
        return {'source_table': tbl, 'status': 'FAILED', 'existed': False, 'error': error_msg}
