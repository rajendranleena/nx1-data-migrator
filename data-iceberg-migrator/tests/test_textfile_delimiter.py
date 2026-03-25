"""
Tests for TEXTFILE delimiter preservation during migration.
"""

from unittest.mock import MagicMock

import migration_dags_combined as m
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_desc_row(col_name, data_type, comment=""):
    """Return a mock row as returned by DESCRIBE FORMATTED."""
    row = MagicMock()
    row.col_name = col_name
    row.data_type = data_type
    row.comment = comment
    return row


def _make_distcp_result(table_overrides=None):
    """Return a minimal distcp_result dict wired for create_hive_tables."""
    table = {
        "source_database": "sales_data",
        "source_table": "transactions",
        "dest_database": "sales_data_s3",
        "dest_bucket": "s3a://test-bucket",
        "source_location": "maprfs:///data/sales_data/transactions",
        "s3_location": "s3a://test-bucket/sales_data_s3/transactions",
        "file_format": "TEXTFILE",
        "serde_properties": {"field.delim": "|"},
        "schema": [
            {"name": "id", "type": "bigint"},
            {"name": "amount", "type": "double"},
            {"name": "name", "type": "string"},
        ],
        "partitions": [],
        "partition_columns": "",
        "partition_count": 0,
        "row_count": 500,
        "is_partitioned": False,
        "unregistered_partitions": False,
        "table_type": "EXTERNAL",
        "source_total_size_bytes": 1024 * 1024,
        "source_file_count": 2,
    }
    if table_overrides:
        table.update(table_overrides)

    return {
        "run_id": "run_test_001",
        "source_database": "sales_data",
        "dest_database": "sales_data_s3",
        "dest_bucket": "s3a://test-bucket",
        "tables": [table],
        "distcp_results": [{
            "source_table": "transactions",
            "status": "COMPLETED",
            "distcp_started_at": "2025-01-01 12:00:00",
            "distcp_completed_at": "2025-01-01 12:05:00",
            "distcp_duration_secs": 300.0,
            "is_incremental": False,
            "bytes_copied": 1024 * 1024,
            "files_copied": 2,
            "s3_total_size_bytes_before": 0,
            "s3_file_count_before": 0,
            "s3_total_size_bytes_after": 1024 * 1024,
            "s3_file_count_after": 2,
            "s3_bytes_transferred": 1024 * 1024,
            "s3_files_transferred": 2,
            "error": None,
        }],
        "_task_duration": 300.0,
    }


def _collect_ddl_calls(mock_spark):
    """Return all SQL strings passed to spark.sql that are CREATE EXTERNAL TABLE statements."""
    return [
        str(c.args[0])
        for c in mock_spark.sql.call_args_list
        if "CREATE EXTERNAL TABLE" in str(c.args[0]).upper()
    ]


# ---------------------------------------------------------------------------
# Stage 1: Discovery – SerDe property extraction
# ---------------------------------------------------------------------------

class TestSerDePropertyExtraction:

    def _spark_table_not_exists(self):
        """Spark mock where DESCRIBE raises (table does not yet exist)."""
        spark = MagicMock()

        def side_effect(sql):
            if sql.strip().upper().startswith("DESCRIBE"):
                raise Exception("Table not found")
            return MagicMock()

        spark.sql.side_effect = side_effect
        return spark

    def test_pipe_delimiter_appears_in_ddl(self):
        """field.delim='|' must produce ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'."""
        spark = self._spark_table_not_exists()
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({"serde_properties": {"field.delim": "|"}}),
            spark=spark,
            ti=MagicMock(),
        )
        ddl_calls = _collect_ddl_calls(spark)
        assert ddl_calls, "No CREATE TABLE DDL was emitted"
        ddl = ddl_calls[0]
        assert "ROW FORMAT DELIMITED" in ddl.upper()
        assert "FIELDS TERMINATED BY '|'" in ddl
        assert result["table_results"][0]["status"] == "COMPLETED"

    def test_comma_delimiter_appears_in_ddl(self):
        """field.delim=',' must produce the correct CSV delimiter clause."""
        spark = self._spark_table_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({"serde_properties": {"field.delim": ","}}),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "FIELDS TERMINATED BY ','" in ddl

    def test_tab_delimiter_appears_in_ddl(self):
        """field.delim='\\t' (tab) must be preserved exactly in the DDL."""
        spark = self._spark_table_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({"serde_properties": {"field.delim": "\t"}}),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "FIELDS TERMINATED BY '\t'" in ddl

    def test_escape_delim_appended_when_present(self):
        """escape.delim must add ESCAPED BY clause after FIELDS TERMINATED BY."""
        spark = self._spark_table_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|", "escape.delim": "\\"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "FIELDS TERMINATED BY '|'" in ddl
        assert "ESCAPED BY" in ddl.upper()

    def test_null_format_produces_tblproperties(self):
        """null.format must be written to TBLPROPERTIES."""
        spark = self._spark_table_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|", "null.format": "\\N"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "TBLPROPERTIES" in ddl.upper()
        assert "serialization.null.format" in ddl
        assert "\\N" in ddl

    def test_field_delimiter_alias_key_accepted(self):
        """'field delimiter' (with space, some Hive versions) should also be captured."""
        spark = self._spark_table_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({"serde_properties": {"field delimiter": "|"}}),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "FIELDS TERMINATED BY '|'" in ddl


# ---------------------------------------------------------------------------
# Stage 2: Table creation – PARQUET tables must NOT get ROW FORMAT clause
# ---------------------------------------------------------------------------

class TestNonTextfileTablesUnchanged:

    def _spark_not_exists(self):
        spark = MagicMock()

        def side_effect(sql):
            if sql.strip().upper().startswith("DESCRIBE"):
                raise Exception("Not found")
            return MagicMock()

        spark.sql.side_effect = side_effect
        return spark

    @pytest.mark.parametrize("fmt", ["PARQUET", "ORC", "AVRO"])
    def test_no_row_format_for_columnar_formats(self, fmt):
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "file_format": fmt,
                "serde_properties": {"field.delim": "|"},
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl_calls = _collect_ddl_calls(spark)
        assert ddl_calls, "No CREATE TABLE DDL was emitted"
        ddl = ddl_calls[0].upper()
        assert "ROW FORMAT" not in ddl
        assert f"STORED AS {fmt}" in ddl

    def test_textfile_without_serde_props_has_no_row_format(self):
        """TEXTFILE table with empty serde_properties must not emit ROW FORMAT."""
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({"serde_properties": {}}),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0].upper()
        assert "ROW FORMAT" not in ddl
        assert "STORED AS TEXTFILE" in ddl

    def test_textfile_missing_serde_key_gracefully_skips_row_format(self):
        """If serde_properties key is absent entirely, no ROW FORMAT emitted."""
        spark = self._spark_not_exists()
        distcp = _make_distcp_result()
        del distcp["tables"][0]["serde_properties"]
        m.create_hive_tables.function.__wrapped__(
            distcp_result=distcp,
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0].upper()
        assert "ROW FORMAT" not in ddl


# ---------------------------------------------------------------------------
# Stage 3: End-to-end DDL structure for TEXTFILE with all properties
# ---------------------------------------------------------------------------

class TestTextfileDdlStructure:
    """Verify the complete DDL structure is syntactically sane."""

    def _spark_not_exists(self):
        spark = MagicMock()

        def side_effect(sql):
            if sql.strip().upper().startswith("DESCRIBE"):
                raise Exception("Not found")
            return MagicMock()

        spark.sql.side_effect = side_effect
        return spark

    def test_ddl_clause_order_is_valid(self):
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|", "null.format": "\\N"},
                "is_partitioned": True,
                "partition_columns": "dt",
                "partitions": ["dt=2024-01-01"],
                "schema": [
                    {"name": "id", "type": "bigint"},
                    {"name": "amount", "type": "double"},
                    {"name": "dt", "type": "string"},
                ],
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        ddl_upper = ddl.upper()

        idx_create    = ddl_upper.index("CREATE EXTERNAL TABLE")
        idx_partition = ddl_upper.index("PARTITIONED BY")
        idx_row       = ddl_upper.index("ROW FORMAT")
        idx_stored    = ddl_upper.index("STORED AS")
        idx_location  = ddl_upper.index("LOCATION")
        idx_tblprop   = ddl_upper.index("TBLPROPERTIES")

        assert idx_create < idx_partition < idx_row < idx_stored < idx_location < idx_tblprop, (
            "DDL clauses are out of order. Hive requires: "
            "CREATE → PARTITIONED BY → ROW FORMAT → STORED AS → LOCATION → TBLPROPERTIES"
        )

    def test_stored_as_textfile_still_present(self):
        """STORED AS TEXTFILE must not be removed by the ROW FORMAT injection."""
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0].upper()
        assert "STORED AS TEXTFILE" in ddl

    def test_location_still_present(self):
        """LOCATION clause must not be displaced by ROW FORMAT injection."""
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "s3a://test-bucket/sales_data_s3/transactions" in ddl

    def test_column_definitions_present(self):
        """Column definitions must survive the DDL rewrite."""
        spark = self._spark_not_exists()
        m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl = _collect_ddl_calls(spark)[0]
        assert "`id` bigint" in ddl
        assert "`amount` double" in ddl
        assert "`name` string" in ddl

    def test_table_creation_status_is_completed(self):
        """create_hive_tables must report COMPLETED for a successful TEXTFILE table."""
        spark = self._spark_not_exists()
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|"}
            }),
            spark=spark,
            ti=MagicMock(),
        )
        assert result["table_results"][0]["status"] == "COMPLETED"
        assert result["table_results"][0]["action"] == "created"
        assert result["_has_failures"] is False


# ---------------------------------------------------------------------------
# Stage 4: Regression – existing table (repair path) is not affected
# ---------------------------------------------------------------------------

class TestRepairPathUnaffected:
    def test_existing_textfile_table_is_repaired_not_recreated(self):
        spark = MagicMock()
        spark.sql.return_value = MagicMock()

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=_make_distcp_result({
                "serde_properties": {"field.delim": "|"},
                "is_partitioned": True,
                "partition_columns": "dt",
                "partitions": ["dt=2024-01-01"],
            }),
            spark=spark,
            ti=MagicMock(),
        )
        ddl_calls = _collect_ddl_calls(spark)
        assert ddl_calls == [], "CREATE TABLE should not be called when table already exists"
        assert result["table_results"][0]["existed"] is True
        assert result["table_results"][0]["status"] == "COMPLETED"
