"""Shared test helpers for building mock data."""

from io import BytesIO
from unittest.mock import MagicMock


def make_excel_bytes(rows):
    """Create a real in-memory Excel file from a list of row dicts."""
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    if rows:
        ws.append(list(rows[0].keys()))
        for row in rows:
            ws.append(list(row.values()))
    buf = BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


def setup_spark_excel(mock_spark, excel_bytes):
    """Wire mock_spark.read.format('binaryFile') to return given bytes."""
    content_row = MagicMock()
    content_row.content = excel_bytes
    df_mock = MagicMock()
    df_mock.select.return_value.first.return_value = content_row
    mock_spark.read.format.return_value.load.return_value = df_mock


def mock_ssh_stdout(exit_code=0, output=b''):
    """Create a mock SSH stdout object with given exit code and output."""
    s = MagicMock()
    s.channel.recv_exit_status.return_value = exit_code
    s.read.return_value = output
    return s
