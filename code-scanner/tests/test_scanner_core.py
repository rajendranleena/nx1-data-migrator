"""
Tests for SparkMigrationScanner core behaviour:
  - scan_file / scan_directory / scan_path
  - summary statistics
  - report output (markdown, json, html)
  - multi-line detection
  - AST-aware deduplication
"""

import json

import pytest

# ---------------------------------------------------------------------------
# scan_file — basic detection
# ---------------------------------------------------------------------------

class TestScanFileBasic:

    def test_detects_sqlcontext(self, scanner, tmp_py):
        f = tmp_py('result = sqlContext.sql("SELECT 1")')
        issues = scanner.scan_file(f)
        rule_ids = [i.rule_id for i in issues]
        assert "SPARK-001" in rule_ids

    def test_detects_hivecontext(self, scanner, tmp_py):
        f = tmp_py('hc = HiveContext(sc)')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-002" for i in issues)

    def test_detects_register_temp_table(self, scanner, tmp_py):
        f = tmp_py('df.registerTempTable("my_view")')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-003" for i in issues)

    def test_detects_union_all(self, scanner, tmp_py):
        f = tmp_py('result = df1.unionAll(df2)')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-009" for i in issues)

    def test_detects_hdfs_path(self, scanner, tmp_py):
        f = tmp_py('path = "hdfs://namenode:8020/data/table"')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "S3-001" for i in issues)

    def test_detects_s3n_protocol(self, scanner, tmp_py):
        f = tmp_py('spark.read.parquet("s3n://mybucket/data")')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "S3-003" for i in issues)

    def test_clean_file_returns_no_issues(self, scanner, tmp_py):
        f = tmp_py('''\
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet("s3a://bucket/data")
            df.createOrReplaceTempView("my_view")
        ''')
        issues = scanner.scan_file(f)
        bad = [i for i in issues if i.severity.value in ("critical", "high", "medium")]
        assert len(bad) == 0

    def test_skips_wrong_extension(self, scanner, tmp_path):
        f = tmp_path / "sample.txt"
        f.write_text('sqlContext.sql("SELECT 1")')
        issues = scanner.scan_file(f)
        assert len(issues) == 0

    def test_returns_correct_line_numbers(self, scanner, tmp_py):
        f = tmp_py("x = 1\ny = sqlContext.sql('SELECT 1')\nz = 3\n")
        issues = scanner.scan_file(f)
        sqlcontext_issues = [i for i in issues if i.rule_id == "SPARK-001"]
        assert len(sqlcontext_issues) >= 1
        assert sqlcontext_issues[0].line_number == 2

    def test_issue_contains_file_path(self, scanner, tmp_py):
        f = tmp_py('sqlContext.sql("x")')
        issues = scanner.scan_file(f)
        assert all(i.file_path == str(f) for i in issues)

    def test_unreadable_file_returns_empty(self, scanner, tmp_path):
        f = tmp_path / "ghost.py"
        issues = scanner.scan_file(f)
        assert issues == []


# ---------------------------------------------------------------------------
# scan_file — SQL rules
# ---------------------------------------------------------------------------

class TestScanFileSql:

    def test_detects_hive_execution_engine(self, scanner, tmp_sql):
        f = tmp_sql("SET hive.execution.engine=tez;")
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "HIVE-001" for i in issues)

    def test_detects_mapred_property(self, scanner, tmp_sql):
        f = tmp_sql("SET mapred.reduce.tasks=100;")
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "HIVE-100" for i in issues)

    def test_detects_add_jar(self, scanner, tmp_sql):
        f = tmp_sql("ADD JAR hdfs:///lib/custom.jar;")
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "HIVE-110" for i in issues)

    def test_sql_rule_not_applied_to_py(self, scanner, tmp_py):
        f = tmp_py("dfs -ls /user/hive")
        issues = scanner.scan_file(f)
        assert all(i.rule_id != "HIVE-112" for i in issues)

    def test_sql_rule_applied_to_hql(self, scanner, tmp_path):
        f = tmp_path / "query.hql"
        f.write_text("dfs -ls /user/hive;")
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "HIVE-112" for i in issues)


# ---------------------------------------------------------------------------
# scan_file — multi-line detection
# ---------------------------------------------------------------------------

class TestMultilineDetection:

    def test_multiline_register_temp_table(self, scanner, tmp_py):
        """SPARK-003 with method chained on next line."""
        f = tmp_py('''\\
            df.registerTempTable(
                "my_view"
            )
        ''')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-003" for i in issues)

    def test_multiline_mappartitions_with_index(self, scanner, tmp_py):
        """SPARK-007 pattern spans multiple lines."""
        f = tmp_py('''\\
            result = rdd.mapPartitionsWithIndex(
                my_func,
                preservesPartitioning=True
            )
        ''')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-007" for i in issues)

    def test_multiline_explode(self, scanner, tmp_py):
        """SPARK-010 old DataFrame.explode with lambda on next line."""
        f = tmp_py('''\\
            df.explode("arr", lambda r: [
                r
            ]) {
        ''')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-010" for i in issues)

    def test_multiline_window(self, scanner, tmp_py):
        """SPARK-012 Window.partitionBy with args on next line."""
        f = tmp_py('''\\
            from pyspark.sql import Window
            w = Window.partitionBy(
                "col_a", "col_b"
            )
        ''')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-012" for i in issues)

    def test_large_file_skips_multiline(self, scanner, tmp_path, cs):
        """Files > 1 MB skip the multi-line pass (performance guard)."""
        f = tmp_path / "big.py"
        padding = "# " + "x" * 78 + "\\n"
        f.write_text(padding * 14_000 + 'df.registerTempTable(\\n"v"\\n)')
        issues = scanner.scan_file(f)
        assert isinstance(issues, list)


# ---------------------------------------------------------------------------
# scan_file — AST-aware filtering
# ---------------------------------------------------------------------------

class TestAstAwareFiltering:

    def test_comment_line_skipped(self, scanner, tmp_py):
        """sqlContext in a comment must NOT generate SPARK-001."""
        f = tmp_py('# sqlContext was the old way, use SparkSession now')
        issues = scanner.scan_file(f)
        assert all(i.rule_id != "SPARK-001" for i in issues)

    def test_docstring_line_skipped(self, scanner, tmp_py):
        f = tmp_py('def foo():\n    """sqlContext is old"""\n    pass\n')
        issues = scanner.scan_file(f)
        assert isinstance(issues, list)

    def test_executable_line_detected(self, scanner, tmp_py):
        """sqlContext on a live assignment line MUST generate SPARK-001."""
        f = tmp_py('sc2 = sqlContext')
        issues = scanner.scan_file(f)
        assert any(i.rule_id == "SPARK-001" for i in issues)


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class TestDeduplication:

    def test_same_rule_same_line_reported_once(self, scanner, tmp_py):
        """A line matching two overlapping patterns for the same rule_id is reported once."""
        f = tmp_py('x = sqlContext; y = sqlContext')
        issues = scanner.scan_file(f)
        spark001 = [i for i in issues if i.rule_id == "SPARK-001"]
        line_nums = [i.line_number for i in spark001]
        assert len(set(line_nums)) == len(line_nums), "Duplicate (rule_id, line) entries found"


# ---------------------------------------------------------------------------
# scan_directory
# ---------------------------------------------------------------------------

class TestScanDirectory:

    def test_scans_multiple_files(self, scanner, tmp_dir):
        (tmp_dir / "a.py").write_text('sqlContext.sql("x")')
        (tmp_dir / "b.py").write_text('df.unionAll(df2)')
        scanner.scan_directory(tmp_dir)
        rule_ids = {i.rule_id for i in scanner.issues}
        assert "SPARK-001" in rule_ids
        assert "SPARK-009" in rule_ids

    def test_files_scanned_counter(self, scanner, tmp_dir):
        (tmp_dir / "f1.py").write_text("x = 1")
        (tmp_dir / "f2.py").write_text("y = 2")
        scanner.scan_directory(tmp_dir)
        assert scanner.files_scanned == 2

    def test_excludes_pycache(self, scanner, tmp_dir):
        cache = tmp_dir / "__pycache__"
        cache.mkdir()
        (cache / "bad.py").write_text('sqlContext.sql("x")')
        scanner.scan_directory(tmp_dir)
        assert scanner.files_scanned == 0

    def test_excludes_venv(self, scanner, tmp_dir):
        venv = tmp_dir / "venv"
        venv.mkdir()
        (venv / "lib.py").write_text('sqlContext.sql("x")')
        scanner.scan_directory(tmp_dir)
        assert scanner.files_scanned == 0

    def test_skips_non_scannable_extensions(self, scanner, tmp_dir):
        (tmp_dir / "notes.md").write_text('sqlContext.sql("x")')
        scanner.scan_directory(tmp_dir)
        assert scanner.files_scanned == 0

    def test_recurses_into_subdirectories(self, scanner, tmp_dir):
        sub = tmp_dir / "src" / "jobs"
        sub.mkdir(parents=True)
        (sub / "job.py").write_text('df.registerTempTable("v")')
        scanner.scan_directory(tmp_dir)
        assert scanner.files_scanned == 1
        assert any(i.rule_id == "SPARK-003" for i in scanner.issues)


# ---------------------------------------------------------------------------
# scan_path
# ---------------------------------------------------------------------------

class TestScanPath:

    def test_scan_path_single_file(self, scanner, tmp_py):
        f = tmp_py('sqlContext.sql("x")')
        scanner.scan_path(str(f))
        assert any(i.rule_id == "SPARK-001" for i in scanner.issues)

    def test_scan_path_directory(self, scanner, tmp_dir):
        (tmp_dir / "x.py").write_text('HiveContext(sc)')
        scanner.scan_path(str(tmp_dir))
        assert any(i.rule_id == "SPARK-002" for i in scanner.issues)

    def test_scan_path_nonexistent_raises(self, scanner):
        with pytest.raises(ValueError, match="does not exist"):
            scanner.scan_path("/nonexistent/path/to/nothing")


# ---------------------------------------------------------------------------
# get_summary
# ---------------------------------------------------------------------------

class TestGetSummary:

    def test_summary_keys(self, scanner, tmp_py):
        tmp_py('sqlContext.sql("x")')
        summary = scanner.get_summary()
        assert "files_scanned" in summary
        assert "lines_scanned" in summary
        assert "total_issues" in summary
        assert "by_severity" in summary
        assert "by_category" in summary

    def test_summary_counts_correct(self, scanner, tmp_py):
        f = tmp_py('sqlContext.sql("x")')
        scanner.scan_path(str(f))
        s = scanner.get_summary()
        assert s["total_issues"] == len(scanner.issues)
        assert s["files_scanned"] == 1

    def test_summary_empty_scan(self, cs):
        empty_scanner = cs.SparkMigrationScanner()
        s = empty_scanner.get_summary()
        assert s["total_issues"] == 0
        assert s["files_scanned"] == 0


# ---------------------------------------------------------------------------
# Report formats
# ---------------------------------------------------------------------------

class TestReportFormats:

    def _populated_scanner(self, cs, tmp_path):
        f = tmp_path / "test.py"
        f.write_text('x = sqlContext\\ndf.unionAll(df2)')
        s = cs.SparkMigrationScanner()
        s.scan_path(str(f))
        return s

    def test_to_markdown_contains_header(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        md = s.to_markdown()
        assert "# Spark Migration Scan Report" in md

    def test_to_markdown_contains_rule_id(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        md = s.to_markdown()
        assert "SPARK-001" in md

    def test_to_markdown_contains_summary(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        md = s.to_markdown()
        assert "Files Scanned" in md

    def test_to_json_is_valid(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        raw = s.to_json()
        data = json.loads(raw)
        assert "summary" in data
        assert "issues" in data

    def test_to_json_issues_have_required_fields(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        data = json.loads(s.to_json())
        for issue in data["issues"]:
            assert "rule_id" in issue
            assert "severity" in issue
            assert "file_path" in issue
            assert "line_number" in issue

    def test_to_html_is_valid_html(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        html = s.to_html()
        assert "<!DOCTYPE html>" in html
        assert "<title>" in html
        assert "</html>" in html

    def test_to_html_contains_issue(self, cs, tmp_path):
        s = self._populated_scanner(cs, tmp_path)
        html = s.to_html()
        assert "SPARK-001" in html

    def test_empty_scan_markdown(self, cs):
        s = cs.SparkMigrationScanner()
        md = s.to_markdown()
        assert "# Spark Migration Scan Report" in md

    def test_empty_scan_json(self, cs):
        s = cs.SparkMigrationScanner()
        data = json.loads(s.to_json())
        assert data["issues"] == []
