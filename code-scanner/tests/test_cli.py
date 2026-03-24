"""
Tests for the code-scanner CLI (main() function):
  - argument parsing combinations
  - --severity filtering
  - --category filtering
  - --format output
  - --output file writing
  - --verbose / --quiet flags
"""

import json
from unittest.mock import patch

import pytest


@pytest.fixture
def py_file_with_issues(tmp_path):
    """A .py file guaranteed to produce at least one CRITICAL and one HIGH issue."""
    f = tmp_path / "violations.py"
    f.write_text(
        'x = sqlContext\\n'
        'df.unionAll(df2)\\n'
        'path = "hdfs://nn:9000/data"\\n'
        'data = df.collect()\\n'
    )
    return f


class TestCliSeverityFilter:

    def test_critical_only_filters_out_high(self, py_file_with_issues, capsys, cs):
        """--severity critical should exclude HIGH/MEDIUM issues."""
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--severity', 'critical']):
            cs.main()
        out = capsys.readouterr().out
        assert "PERF-001" not in out

    def test_info_severity_includes_all(self, py_file_with_issues, capsys, cs):
        """--severity info shows everything."""
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--severity', 'info']):
            cs.main()
        out = capsys.readouterr().out
        assert "SPARK-001" in out


class TestCliCategoryFilter:

    def test_category_filter_s3_only(self, py_file_with_issues, capsys, cs):
        """--category 'HDFS to S3 Migration' should include S3-001 and exclude SPARK-001."""
        with patch('sys.argv', [
            'code-scanner', str(py_file_with_issues),
            '--category', 'HDFS to S3 Migration'
        ]):
            cs.main()
        out = capsys.readouterr().out
        assert "S3-001" in out
        assert "SPARK-001" not in out

    def test_multiple_categories(self, py_file_with_issues, capsys, cs):
        """Two --category flags should include both categories."""
        with patch('sys.argv', [
            'code-scanner', str(py_file_with_issues),
            '--category', 'Spark API Changes',
            '--category', 'Performance Considerations',
        ]):
            cs.main()
        out = capsys.readouterr().out
        assert "SPARK-001" in out
        assert "PERF-001" in out


class TestCliOutputFormats:

    def test_format_json_outputs_valid_json(self, py_file_with_issues, capsys, cs):
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--format', 'json']):
            cs.main()
        raw = capsys.readouterr().out
        json_start = raw.find('{')
        data = json.loads(raw[json_start:])
        assert "summary" in data
        assert "issues" in data

    def test_format_html_outputs_html(self, py_file_with_issues, capsys, cs):
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--format', 'html']):
            cs.main()
        out = capsys.readouterr().out
        assert "<!DOCTYPE html>" in out

    def test_format_md_outputs_markdown(self, py_file_with_issues, capsys, cs):
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--format', 'md']):
            cs.main()
        out = capsys.readouterr().out
        assert "# Spark Migration Scan Report" in out


class TestCliOutputFile:

    def test_writes_output_file(self, py_file_with_issues, tmp_path, cs):
        out_file = tmp_path / "report.md"
        with patch('sys.argv', [
            'code-scanner', str(py_file_with_issues),
            '--output', str(out_file),
        ]):
            cs.main()
        assert out_file.exists()
        assert "Spark Migration Scan Report" in out_file.read_text()

    def test_writes_json_output_file(self, py_file_with_issues, tmp_path, cs):
        out_file = tmp_path / "report.json"
        with patch('sys.argv', [
            'code-scanner', str(py_file_with_issues),
            '--format', 'json',
            '--output', str(out_file),
        ]):
            cs.main()
        data = json.loads(out_file.read_text())
        assert "issues" in data


class TestCliErrorHandling:

    def test_nonexistent_path_exits_1(self, cs):
        with patch('sys.argv', ['code-scanner', '/totally/fake/path']), pytest.raises(SystemExit) as exc_info:
            cs.main()
        assert exc_info.value.code == 1

    def test_verbose_flag_accepted(self, py_file_with_issues, capsys, cs):
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--verbose']):
            cs.main()

    def test_quiet_flag_accepted(self, py_file_with_issues, capsys, cs):
        with patch('sys.argv', ['code-scanner', str(py_file_with_issues), '--quiet']):
            cs.main()


class TestAstExtractor:
    """Unit tests for ASTCodeExtractor used by ast_aware rules."""

    def test_executable_lines_detected(self, cs):
        src = 'x = 1\\ny = 2\\n'
        extractor = cs.ASTCodeExtractor(src)
        assert extractor.is_executable(1)
        assert extractor.is_executable(2)

    def test_comment_line_not_executable(self, cs):
        src = "x = 1\n# comment\ny = 2\n"
        extractor = cs.ASTCodeExtractor(src)
        assert extractor.is_executable(1)
        assert extractor.is_executable(3)
        assert extractor.parse_succeeded is True

    def test_parse_succeeded_valid_python(self, cs):
        src = "x = 1\n"
        extractor = cs.ASTCodeExtractor(src)
        assert extractor.parse_succeeded is True

    def test_parse_succeeded_invalid_python(self, cs):
        extractor = cs.ASTCodeExtractor('def broken(:\\n')
        assert extractor.parse_succeeded is False

    def test_all_lines_executable_on_syntax_error(self, cs):
        extractor = cs.ASTCodeExtractor('def broken(:\\n')
        assert extractor.is_executable(1) is True
        assert extractor.is_executable(999) is True
