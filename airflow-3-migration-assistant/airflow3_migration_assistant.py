#!/usr/bin/env python3
"""
Airflow 3 Migration Assistant
Scans Airflow 2 DAG files and converts them to Airflow 3 compatible code.
Supports dry-run mode (report only) and auto-fix mode (applies changes).
Generates an HTML report matching the project's existing report style.
"""

import argparse
import ast
import os
import re
import sys
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Migration Rule Definitions
# ---------------------------------------------------------------------------

@dataclass
class MigrationFinding:
    """A single migration issue found in a DAG file."""
    file: str
    line: int
    category: str
    severity: str  # BREAKING, DEPRECATED, WARNING
    old_code: str
    new_code: str
    description: str
    auto_fixable: bool = True
    applied: bool = False


# --- Import replacement rules -----------------------------------------------

IMPORT_REPLACEMENTS = [
    # 1. Decorators & DAG → airflow.sdk
    {
        "pattern": r"from\s+airflow\.decorators\s+import\s+(.+)",
        "handler": "sdk_decorator_import",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "Decorators moved from airflow.decorators to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\s+import\s+DAG\b",
        "replacement": "from airflow.sdk import DAG",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "DAG import moved from airflow to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.models\.dag\s+import\s+DAG\b",
        "replacement": "from airflow.sdk import DAG",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "DAG import moved from airflow.models.dag to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.models\s+import\s+Variable\b",
        "replacement": "from airflow.sdk import Variable",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "Variable moved from airflow.models to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.models\.param\s+import\s+Param\b",
        "replacement": "from airflow.sdk.definitions.param import Param",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "Param moved to airflow.sdk.definitions.param",
    },
    {
        "pattern": r"from\s+airflow\.models\.param\s+import\s+ParamsDict\b",
        "replacement": "from airflow.sdk.definitions.param import ParamsDict",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "ParamsDict moved to airflow.sdk.definitions.param",
    },
    # Base classes → airflow.sdk
    {
        "pattern": r"from\s+airflow\.models\s+import\s+BaseOperator\b",
        "replacement": "from airflow.sdk import BaseOperator",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "BaseOperator moved to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.models\.baseoperator\s+import\s+BaseOperator\b",
        "replacement": "from airflow.sdk import BaseOperator",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "BaseOperator moved to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.models\.baseoperator\s+import\s+chain\b",
        "replacement": "from airflow.sdk import chain",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "chain() moved to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.base\s+import\s+BaseSensorOperator\b",
        "replacement": "from airflow.sdk.bases.sensor import BaseSensorOperator",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "BaseSensorOperator moved to airflow.sdk.bases.sensor",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.base_sensor_operator\s+import\s+BaseSensorOperator\b",
        "replacement": "from airflow.sdk.bases.sensor import BaseSensorOperator",
        "category": "SDK Migration",
        "severity": "BREAKING",
        "description": "BaseSensorOperator old path removed; use airflow.sdk.bases.sensor",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.base\s+import\s+PokeReturnValue\b",
        "replacement": "from airflow.sdk.bases.sensor import PokeReturnValue",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "PokeReturnValue moved to airflow.sdk.bases.sensor",
    },
    {
        "pattern": r"from\s+airflow\.hooks\.base\s+import\s+BaseHook\b",
        "replacement": "from airflow.sdk.bases.hook import BaseHook",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "BaseHook moved to airflow.sdk.bases.hook",
    },
    {
        "pattern": r"from\s+airflow\.utils\.task_group\s+import\s+TaskGroup\b",
        "replacement": "from airflow.sdk import TaskGroup",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "TaskGroup moved to airflow.sdk",
    },
    {
        "pattern": r"from\s+airflow\.decorators\s+import\s+task_group\b",
        "replacement": "from airflow.sdk import task_group",
        "category": "SDK Migration",
        "severity": "DEPRECATED",
        "description": "task_group moved to airflow.sdk",
    },
    # 2. Datasets → Assets
    {
        "pattern": r"from\s+airflow\.datasets\s+import\s+Dataset\b",
        "replacement": "from airflow.sdk import Asset",
        "category": "Assets Rename",
        "severity": "BREAKING",
        "description": "Dataset renamed to Asset in Airflow 3",
    },
    {
        "pattern": r"from\s+airflow\.datasets\s+import\s+DatasetAlias\b",
        "replacement": "from airflow.sdk import AssetAlias",
        "category": "Assets Rename",
        "severity": "BREAKING",
        "description": "DatasetAlias renamed to AssetAlias",
    },
    {
        "pattern": r"from\s+airflow\.datasets\s+import\s+DatasetAll\b",
        "replacement": "from airflow.sdk import AssetAll",
        "category": "Assets Rename",
        "severity": "BREAKING",
        "description": "DatasetAll renamed to AssetAll",
    },
    {
        "pattern": r"from\s+airflow\.datasets\s+import\s+DatasetAny\b",
        "replacement": "from airflow.sdk import AssetAny",
        "category": "Assets Rename",
        "severity": "BREAKING",
        "description": "DatasetAny renamed to AssetAny",
    },
    {
        "pattern": r"from\s+airflow\.datasets\.metadata\s+import\s+Metadata\b",
        "replacement": "from airflow.sdk import Metadata",
        "category": "Assets Rename",
        "severity": "BREAKING",
        "description": "Metadata moved to airflow.sdk",
    },
    # 3. Operators → standard provider
    {
        "pattern": r"from\s+airflow\.operators\.bash\s+import\s+BashOperator\b",
        "replacement": "from airflow.providers.standard.operators.bash import BashOperator",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "BashOperator moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.python\s+import\s+PythonOperator\b",
        "replacement": "from airflow.providers.standard.operators.python import PythonOperator",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "PythonOperator moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.python\s+import\s+(.+)",
        "handler": "standard_python_import",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "Python operators moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.empty\s+import\s+EmptyOperator\b",
        "replacement": "from airflow.providers.standard.operators.empty import EmptyOperator",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "EmptyOperator moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.dummy\s+import\s+DummyOperator\b",
        "replacement": "from airflow.providers.standard.operators.empty import EmptyOperator",
        "category": "Standard Provider",
        "severity": "BREAKING",
        "description": "DummyOperator removed; use EmptyOperator from standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.trigger_dagrun\s+import\s+TriggerDagRunOperator\b",
        "replacement": "from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "TriggerDagRunOperator moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.datetime\s+import\s+(.+)",
        "handler": "standard_datetime_import",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "Datetime operators moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.operators\.latest_only\s+import\s+LatestOnlyOperator\b",
        "replacement": "from airflow.providers.standard.operators.latest_only import LatestOnlyOperator",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "LatestOnlyOperator moved to standard provider",
    },
    # 4. Sensors → standard provider
    {
        "pattern": r"from\s+airflow\.sensors\.python\s+import\s+(.+)",
        "handler": "standard_sensor_import",
        "source_module": "python",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "Python sensors moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.bash\s+import\s+(.+)",
        "handler": "standard_sensor_import",
        "source_module": "bash",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "Bash sensors moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.date_time\s+import\s+(.+)",
        "handler": "standard_sensor_import",
        "source_module": "date_time",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "DateTime sensors moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.time_delta\s+import\s+(.+)",
        "handler": "standard_sensor_import",
        "source_module": "time_delta",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "TimeDelta sensors moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.sensors\.external_task\s+import\s+(.+)",
        "handler": "standard_sensor_import",
        "source_module": "external_task",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "ExternalTask sensors moved to standard provider",
    },
    # 5. Hooks → standard provider
    {
        "pattern": r"from\s+airflow\.hooks\.filesystem\s+import\s+FSHook\b",
        "replacement": "from airflow.providers.standard.hooks.filesystem import FSHook",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "FSHook moved to standard provider",
    },
    {
        "pattern": r"from\s+airflow\.hooks\.subprocess\s+import\s+SubprocessHook\b",
        "replacement": "from airflow.providers.standard.hooks.subprocess import SubprocessHook",
        "category": "Standard Provider",
        "severity": "DEPRECATED",
        "description": "SubprocessHook moved to standard provider",
    },
    # 6. KubernetesPodOperator path change
    {
        "pattern": r"from\s+airflow\.providers\.cncf\.kubernetes\.operators\.kubernetes_pod\s+import\s+KubernetesPodOperator\b",
        "replacement": "from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator",
        "category": "K8s Operator",
        "severity": "BREAKING",
        "description": "KubernetesPodOperator moved from .kubernetes_pod to .pod",
    },
    # 7. Database-specific operators → SQLExecuteQueryOperator
    {
        "pattern": r"from\s+airflow\.providers\.trino\.operators\.trino\s+import\s+TrinoOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "TrinoOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    {
        "pattern": r"from\s+airflow\.providers\.postgres\.operators\.postgres\s+import\s+PostgresOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "PostgresOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    {
        "pattern": r"from\s+airflow\.providers\.mysql\.operators\.mysql\s+import\s+MySqlOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "MySqlOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    {
        "pattern": r"from\s+airflow\.providers\.microsoft\.mssql\.operators\.mssql\s+import\s+MsSqlOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "MsSqlOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    {
        "pattern": r"from\s+airflow\.providers\.snowflake\.operators\.snowflake\s+import\s+SnowflakeOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "SnowflakeOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    {
        "pattern": r"from\s+airflow\.providers\.apache\.hive\.operators\.hive\s+import\s+HiveOperator\b",
        "replacement": "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator",
        "category": "SQL Operator",
        "severity": "BREAKING",
        "description": "HiveOperator removed; use SQLExecuteQueryOperator with conn_id",
    },
    # 8. Removed utilities
    {
        "pattern": r"from\s+airflow\.utils\.dates\s+import\s+days_ago\b",
        "replacement": "# REMOVED: days_ago - use pendulum.today('UTC').add(days=-N) instead",
        "category": "Removed Utility",
        "severity": "BREAKING",
        "description": "days_ago removed; use pendulum.today('UTC').add(days=-N)",
    },
]

# --- Operator class name replacements (for instantiation) ------------------

OPERATOR_CLASS_RENAMES = {
    "DummyOperator": "EmptyOperator",
    "TrinoOperator": "SQLExecuteQueryOperator",
    "PostgresOperator": "SQLExecuteQueryOperator",
    "MySqlOperator": "SQLExecuteQueryOperator",
    "MsSqlOperator": "SQLExecuteQueryOperator",
    "SnowflakeOperator": "SQLExecuteQueryOperator",
    "HiveOperator": "SQLExecuteQueryOperator",
}

# --- Connection parameter renames (for SQL operator migration) -------------

CONN_ID_RENAMES = {
    "trino_conn_id": "conn_id",
    "postgres_conn_id": "conn_id",
    "mysql_conn_id": "conn_id",
    "mssql_conn_id": "conn_id",
    "snowflake_conn_id": "conn_id",
    "hive_cli_conn_id": "conn_id",
}

# --- DAG parameter deprecations -------------------------------------------

DAG_PARAM_CHANGES = [
    {
        "pattern": r"\bschedule_interval\s*=",
        "replacement_key": "schedule",
        "category": "DAG Parameters",
        "severity": "BREAKING",
        "description": "schedule_interval removed; use schedule parameter",
    },
    {
        "pattern": r"\btimetable\s*=",
        "replacement_key": "schedule",
        "category": "DAG Parameters",
        "severity": "BREAKING",
        "description": "timetable parameter removed; use schedule parameter",
    },
    {
        "pattern": r"\btask_concurrency\s*=",
        "replacement_key": "max_active_tis_per_dag",
        "category": "DAG Parameters",
        "severity": "BREAKING",
        "description": "task_concurrency removed; use max_active_tis_per_dag",
    },
]

# --- Context variable deprecations ----------------------------------------

CONTEXT_REMOVALS = {
    "execution_date": ("logical_date", "execution_date renamed to logical_date"),
    "tomorrow_ds": (None, "tomorrow_ds removed; compute manually"),
    "tomorrow_ds_nodash": (None, "tomorrow_ds_nodash removed; compute manually"),
    "yesterday_ds": (None, "yesterday_ds removed; compute manually"),
    "yesterday_ds_nodash": (None, "yesterday_ds_nodash removed; compute manually"),
    "prev_ds": (None, "prev_ds removed; compute manually"),
    "prev_ds_nodash": (None, "prev_ds_nodash removed; compute manually"),
    "prev_execution_date": (None, "prev_execution_date removed; compute manually"),
    "prev_execution_date_success": (None, "prev_execution_date_success removed; compute manually"),
    "next_execution_date": (None, "next_execution_date removed; compute manually"),
    "next_ds": (None, "next_ds removed; compute manually"),
    "next_ds_nodash": (None, "next_ds_nodash removed; compute manually"),
}

# --- Trigger rule changes --------------------------------------------------

TRIGGER_RULE_RENAMES = {
    "NONE_FAILED_OR_SKIPPED": ("NONE_FAILED_MIN_ONE_SUCCESS", "TriggerRule.NONE_FAILED_OR_SKIPPED removed"),
    "DUMMY": ("ALWAYS", "TriggerRule.DUMMY removed; use ALWAYS"),
}


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class Airflow3MigrationScanner:
    """Scans DAG files for Airflow 2 → 3 migration issues."""

    def __init__(self, dag_directory: str, dry_run: bool = True):
        self.dag_directory = Path(dag_directory)
        self.dry_run = dry_run
        self.findings: list[MigrationFinding] = []
        self.files_scanned = 0
        self.files_with_issues = 0

    # Files/directories to skip during scanning
    EXCLUDE_PATTERNS = {
        "__pycache__",
        ".git",
        "airflow3_migration_assistant.py",
        ".airflow2.bak",
    }

    def _should_skip(self, file_path: Path) -> bool:
        """Check if a file should be excluded from scanning."""
        parts = file_path.parts
        name = file_path.name
        for pattern in self.EXCLUDE_PATTERNS:
            if pattern in parts or name == pattern or name.endswith(".bak"):
                return True
        return False

    def scan(self) -> list[MigrationFinding]:
        """Scan all Python files in the DAG directory."""
        py_files = [f for f in self.dag_directory.rglob("*.py") if not self._should_skip(f)]
        self.files_scanned = len(py_files)
        for py_file in py_files:
            self._scan_file(py_file)
        self.files_with_issues = len(set(f.file for f in self.findings))
        return self.findings

    def _scan_file(self, file_path: Path):
        """Scan a single file for all migration issues."""
        try:
            content = file_path.read_text(encoding="utf-8")
        except Exception:
            return

        lines = content.splitlines()
        rel_path = str(file_path.relative_to(self.dag_directory))

        for line_num, line in enumerate(lines, start=1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # Check import replacements
            self._check_imports(rel_path, line_num, stripped, line)

            # Check DAG parameter deprecations
            self._check_dag_params(rel_path, line_num, stripped, line)

            # Check context variable usage
            self._check_context_vars(rel_path, line_num, stripped, line)

            # Check trigger rule renames
            self._check_trigger_rules(rel_path, line_num, stripped, line)

            # Check operator class renames (instantiation)
            self._check_operator_renames(rel_path, line_num, stripped, line)

            # Check connection id parameter renames
            self._check_conn_id_renames(rel_path, line_num, stripped, line)

            # Check @task.pyspark sc parameter
            self._check_pyspark_decorator(rel_path, line_num, stripped, line)

            # Check days_ago usage
            self._check_days_ago_usage(rel_path, line_num, stripped, line)

            # Check Dataset usage (class names, not imports)
            self._check_dataset_usage(rel_path, line_num, stripped, line)

            # Check use_dill parameter
            self._check_use_dill(rel_path, line_num, stripped, line)

    def _check_imports(self, file: str, line_num: int, stripped: str, original: str):
        for rule in IMPORT_REPLACEMENTS:
            match = re.match(rule["pattern"], stripped)
            if not match:
                continue

            if "handler" in rule:
                new_code = self._handle_import(rule, match, stripped)
            else:
                new_code = rule["replacement"]

            if new_code and new_code != stripped:
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category=rule["category"],
                    severity=rule["severity"],
                    old_code=stripped,
                    new_code=new_code,
                    description=rule["description"],
                ))
            break  # Only match first rule per line

    def _handle_import(self, rule: dict, match: re.Match, original: str) -> str:
        handler = rule["handler"]
        if handler == "sdk_decorator_import":
            names = match.group(1).strip()
            return f"from airflow.sdk import {names}"
        elif handler == "standard_python_import":
            names = match.group(1).strip()
            return f"from airflow.providers.standard.operators.python import {names}"
        elif handler == "standard_datetime_import":
            names = match.group(1).strip()
            return f"from airflow.providers.standard.operators.datetime import {names}"
        elif handler == "standard_sensor_import":
            names = match.group(1).strip()
            module = rule.get("source_module", "")
            return f"from airflow.providers.standard.sensors.{module} import {names}"
        return original

    def _check_dag_params(self, file: str, line_num: int, stripped: str, original: str):
        for rule in DAG_PARAM_CHANGES:
            match = re.search(rule["pattern"], stripped)
            if match:
                old_key = match.group(0).rstrip("= ")
                new_code = stripped.replace(old_key, rule["replacement_key"])
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category=rule["category"],
                    severity=rule["severity"],
                    old_code=stripped,
                    new_code=new_code,
                    description=rule["description"],
                ))

    def _check_context_vars(self, file: str, line_num: int, stripped: str, original: str):
        for old_key, (new_key, desc) in CONTEXT_REMOVALS.items():
            # Match context['key'], context["key"], context.get('key'), **context patterns
            patterns = [
                rf"""context\s*\[\s*['\"]({re.escape(old_key)})['\"]""",
                rf"""context\.get\s*\(\s*['\"]({re.escape(old_key)})['\"]\s*""",
                rf"""\bkwargs\s*\[\s*['\"]({re.escape(old_key)})['\"]""",
                rf"""\b({re.escape(old_key)})\s*=\s*context""",
            ]
            for pat in patterns:
                if re.search(pat, stripped):
                    if new_key:
                        new_code = stripped.replace(old_key, new_key)
                    else:
                        new_code = f"# MANUAL FIX REQUIRED: {desc}"
                    self.findings.append(MigrationFinding(
                        file=file,
                        line=line_num,
                        category="Context Variables",
                        severity="BREAKING",
                        old_code=stripped,
                        new_code=new_code,
                        description=desc,
                        auto_fixable=new_key is not None,
                    ))
                    break

    def _check_trigger_rules(self, file: str, line_num: int, stripped: str, original: str):
        for old_val, (new_val, desc) in TRIGGER_RULE_RENAMES.items():
            # Only match TriggerRule.X usage, not dictionary keys or string definitions
            if f"TriggerRule.{old_val}" in stripped:
                new_code = stripped.replace(f"TriggerRule.{old_val}", f"TriggerRule.{new_val}")
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category="Trigger Rules",
                    severity="BREAKING",
                    old_code=stripped,
                    new_code=new_code,
                    description=desc,
                ))

    def _check_operator_renames(self, file: str, line_num: int, stripped: str, original: str):
        for old_name, new_name in OPERATOR_CLASS_RENAMES.items():
            # Match class instantiation: OldOperator(
            if re.search(rf"\b{old_name}\s*\(", stripped):
                new_code = re.sub(rf"\b{old_name}\b", new_name, stripped)
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category="Operator Rename",
                    severity="BREAKING",
                    old_code=stripped,
                    new_code=new_code,
                    description=f"{old_name} renamed/replaced by {new_name}",
                ))

    def _check_conn_id_renames(self, file: str, line_num: int, stripped: str, original: str):
        for old_param, new_param in CONN_ID_RENAMES.items():
            if re.search(rf"\b{old_param}\s*=", stripped):
                new_code = re.sub(rf"\b{old_param}\b", new_param, stripped)
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category="Connection Parameters",
                    severity="BREAKING",
                    old_code=stripped,
                    new_code=new_code,
                    description=f"{old_param} renamed to conn_id for SQLExecuteQueryOperator",
                ))

    def _check_pyspark_decorator(self, file: str, line_num: int, stripped: str, original: str):
        # Detect def func(spark, sc): pattern after @task.pyspark
        if re.match(r"def\s+\w+\s*\(\s*spark\s*,\s*sc\s*", stripped):
            new_code = re.sub(r"\(\s*spark\s*,\s*sc\s*", "(spark", stripped)
            self.findings.append(MigrationFinding(
                file=file,
                line=line_num,
                category="PySpark Decorator",
                severity="BREAKING",
                old_code=stripped,
                new_code=new_code,
                description="@task.pyspark no longer passes sc; use spark.sparkContext if needed",
            ))

    def _check_days_ago_usage(self, file: str, line_num: int, stripped: str, original: str):
        match = re.search(r"\bdays_ago\s*\(\s*(\d+)\s*\)", stripped)
        if match:
            n = match.group(1)
            old = match.group(0)
            new = f"pendulum.today('UTC').add(days=-{n})"
            new_code = stripped.replace(old, new)
            self.findings.append(MigrationFinding(
                file=file,
                line=line_num,
                category="Removed Utility",
                severity="BREAKING",
                old_code=stripped,
                new_code=new_code,
                description=f"days_ago() removed; use pendulum",
            ))

    def _check_dataset_usage(self, file: str, line_num: int, stripped: str, original: str):
        # Check for Dataset( instantiation (not in import lines)
        if not stripped.startswith("from ") and not stripped.startswith("import "):
            if re.search(r"\bDataset\s*\(", stripped):
                new_code = re.sub(r"\bDataset\b", "Asset", stripped)
                self.findings.append(MigrationFinding(
                    file=file,
                    line=line_num,
                    category="Assets Rename",
                    severity="BREAKING",
                    old_code=stripped,
                    new_code=new_code,
                    description="Dataset renamed to Asset in Airflow 3",
                ))

    def _check_use_dill(self, file: str, line_num: int, stripped: str, original: str):
        if re.search(r"\buse_dill\s*=", stripped):
            new_code = re.sub(r",?\s*use_dill\s*=\s*(True|False)", "", stripped)
            self.findings.append(MigrationFinding(
                file=file,
                line=line_num,
                category="Removed Parameters",
                severity="BREAKING",
                old_code=stripped,
                new_code=new_code,
                description="use_dill parameter removed from PythonOperator",
            ))


# ---------------------------------------------------------------------------
# Fixer
# ---------------------------------------------------------------------------

class Airflow3MigrationFixer:
    """Applies migration fixes to DAG files."""

    def __init__(self, dag_directory: str, findings: list[MigrationFinding], backup: bool = True):
        self.dag_directory = Path(dag_directory)
        self.findings = findings
        self.backup = backup
        self.files_modified = 0

    def apply(self) -> int:
        """Apply all auto-fixable findings. Returns count of applied fixes."""
        fixable = [f for f in self.findings if f.auto_fixable]
        # Group by file
        by_file: dict[str, list[MigrationFinding]] = {}
        for f in fixable:
            by_file.setdefault(f.file, []).append(f)

        applied = 0
        for rel_path, file_findings in by_file.items():
            full_path = self.dag_directory / rel_path
            try:
                content = full_path.read_text(encoding="utf-8")
            except Exception:
                continue

            if self.backup:
                backup_path = full_path.with_suffix(full_path.suffix + ".airflow2.bak")
                shutil.copy2(full_path, backup_path)

            lines = content.splitlines()
            # Sort findings by line number descending so edits don't shift indices
            file_findings.sort(key=lambda x: x.line, reverse=True)

            for finding in file_findings:
                idx = finding.line - 1
                if 0 <= idx < len(lines):
                    old_stripped = lines[idx].strip()
                    if old_stripped == finding.old_code:
                        # Preserve indentation
                        indent = lines[idx][: len(lines[idx]) - len(lines[idx].lstrip())]
                        lines[idx] = indent + finding.new_code
                        finding.applied = True
                        applied += 1

            full_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            self.files_modified += 1

        return applied


# ---------------------------------------------------------------------------
# Report Generator
# ---------------------------------------------------------------------------

def generate_html_report(
    dag_directory: str,
    findings: list[MigrationFinding],
    files_scanned: int,
    files_with_issues: int,
    dry_run: bool,
    output_path: Optional[str] = None,
) -> str:
    """Generate an HTML report matching the project's existing report style."""
    from collections import Counter

    total_findings = len(findings)
    breaking = sum(1 for f in findings if f.severity == "BREAKING")
    deprecated = sum(1 for f in findings if f.severity == "DEPRECATED")
    warnings = sum(1 for f in findings if f.severity == "WARNING")
    auto_fixable = sum(1 for f in findings if f.auto_fixable)
    manual_required = total_findings - auto_fixable
    applied = sum(1 for f in findings if f.applied)

    category_counts = Counter(f.category for f in findings)
    severity_counts = Counter(f.severity for f in findings)
    file_counts = Counter(f.file for f in findings)

    mode_label = "DRY RUN" if dry_run else "MIGRATION APPLIED"
    mode_class = "status-partial" if dry_run else "status-success"

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Airflow 3 Migration Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px; }}
        .container {{ background: #fff; padding: 20px; border-radius: 8px; max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        .summary-cards {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 20px 0; }}
        .summary-card {{ padding: 15px 20px; border-radius: 6px; color: #fff; min-width: 150px; text-align: center; }}
        .success {{ background-color: #28a745; }}
        .failed {{ background-color: #dc3545; }}
        .info {{ background-color: #17a2b8; }}
        .warning {{ background-color: #ffc107; color: #212529; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background-color: #343a40; color: white; }}
        tr:nth-child(even) {{ background-color: #f8f9fa; }}
        tr:hover {{ background-color: #e9ecef; }}
        .status-badge {{ padding: 4px 10px; border-radius: 12px; font-weight: bold; color: white; font-size: 12px; }}
        .status-success {{ background-color: #28a745; }}
        .status-failed {{ background-color: #dc3545; }}
        .status-partial {{ background-color: #ffc107; color: #212529; }}
        .status-breaking {{ background-color: #dc3545; }}
        .status-deprecated {{ background-color: #fd7e14; }}
        .status-warning {{ background-color: #ffc107; color: #212529; }}
        .status-applied {{ background-color: #28a745; }}
        .run-info {{ background: #f8f9fa; padding: 15px; border-radius: 6px; margin-bottom: 20px; }}
        .run-info p {{ margin: 5px 0; }}
        code {{ background: #e9ecef; padding: 2px 6px; border-radius: 3px; font-size: 13px; }}
        .code-cell {{ font-family: 'Courier New', monospace; font-size: 12px; max-width: 500px; word-wrap: break-word; }}
        .arrow {{ color: #28a745; font-weight: bold; font-size: 16px; }}
        .category-chart {{ display: flex; flex-wrap: wrap; gap: 8px; margin: 10px 0; }}
        .category-tag {{ padding: 6px 14px; border-radius: 16px; font-size: 13px; font-weight: bold; }}
        .ct-sdk {{ background: #e3f2fd; color: #1565c0; }}
        .ct-provider {{ background: #fff3e0; color: #e65100; }}
        .ct-breaking {{ background: #ffebee; color: #c62828; }}
        .ct-params {{ background: #f3e5f5; color: #6a1b9a; }}
        .ct-context {{ background: #fce4ec; color: #880e4f; }}
        .ct-other {{ background: #e8eaf6; color: #283593; }}
        ul.clean {{ list-style: none; padding: 0; }}
        ul.clean li {{ padding: 4px 0; }}
        .file-issue-count {{ font-weight: bold; color: #dc3545; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Airflow 3 Migration Report</h1>

        <div class="run-info">
            <p><strong>DAG Directory:</strong> {dag_directory}</p>
            <p><strong>Scan Date:</strong> {timestamp} UTC</p>
            <p><strong>Mode:</strong> <span class="status-badge {mode_class}">{mode_label}</span></p>
        </div>

        <h2>Summary</h2>
        <div class="summary-cards">
            <div class="summary-card info">Files Scanned<br><strong>{files_scanned}</strong></div>
            <div class="summary-card {'failed' if files_with_issues > 0 else 'success'}">Files With Issues<br><strong>{files_with_issues}</strong></div>
            <div class="summary-card info">Total Findings<br><strong>{total_findings}</strong></div>
        </div>

        <div class="summary-cards">
            <div class="summary-card failed">Breaking<br><strong>{breaking}</strong></div>
            <div class="summary-card warning">Deprecated<br><strong>{deprecated}</strong></div>
            <div class="summary-card info">Warnings<br><strong>{warnings}</strong></div>
        </div>

        <div class="summary-cards">
            <div class="summary-card success">Auto-Fixable<br><strong>{auto_fixable}</strong></div>
            <div class="summary-card failed">Manual Fix Required<br><strong>{manual_required}</strong></div>
            <div class="summary-card {'success' if applied > 0 else 'info'}">Applied<br><strong>{applied}</strong></div>
        </div>

        <h2>Issues By Category</h2>
        <div class="category-chart">
"""
    category_css = {
        "SDK Migration": "ct-sdk",
        "Standard Provider": "ct-provider",
        "Assets Rename": "ct-breaking",
        "DAG Parameters": "ct-params",
        "Context Variables": "ct-context",
    }
    for cat, count in category_counts.most_common():
        css = category_css.get(cat, "ct-other")
        html += f'            <span class="category-tag {css}">{cat}: {count}</span>\n'

    html += """        </div>

        <h2>Issues By File</h2>
        <table>
            <thead>
                <tr>
                    <th>File</th>
                    <th>Issues</th>
                    <th>Breaking</th>
                    <th>Deprecated</th>
                    <th>Auto-Fixable</th>
                </tr>
            </thead>
            <tbody>
"""
    for file_path, count in file_counts.most_common():
        file_findings = [f for f in findings if f.file == file_path]
        b = sum(1 for f in file_findings if f.severity == "BREAKING")
        d = sum(1 for f in file_findings if f.severity == "DEPRECATED")
        af = sum(1 for f in file_findings if f.auto_fixable)
        html += f"""                <tr>
                    <td><code>{file_path}</code></td>
                    <td class="file-issue-count">{count}</td>
                    <td>{'<span class="status-badge status-breaking">' + str(b) + '</span>' if b > 0 else '0'}</td>
                    <td>{'<span class="status-badge status-deprecated">' + str(d) + '</span>' if d > 0 else '0'}</td>
                    <td>{af}</td>
                </tr>
"""
    html += """            </tbody>
        </table>

        <h2>Detailed Findings</h2>
        <table>
            <thead>
                <tr>
                    <th>File</th>
                    <th>Line</th>
                    <th>Category</th>
                    <th>Severity</th>
                    <th>Old Code</th>
                    <th>New Code</th>
                    <th>Description</th>
                    <th>Status</th>
                </tr>
            </thead>
            <tbody>
"""
    for f in sorted(findings, key=lambda x: (x.file, x.line)):
        sev_class = {
            "BREAKING": "status-breaking",
            "DEPRECATED": "status-deprecated",
            "WARNING": "status-warning",
        }.get(f.severity, "status-partial")

        if f.applied:
            status = '<span class="status-badge status-applied">APPLIED</span>'
        elif f.auto_fixable:
            status = '<span class="status-badge status-partial">FIXABLE</span>'
        else:
            status = '<span class="status-badge status-breaking">MANUAL</span>'

        # HTML-escape the code
        import html as html_module
        old_esc = html_module.escape(f.old_code)
        new_esc = html_module.escape(f.new_code)

        html += f"""                <tr>
                    <td><code>{f.file}</code></td>
                    <td>{f.line}</td>
                    <td>{f.category}</td>
                    <td><span class="status-badge {sev_class}">{f.severity}</span></td>
                    <td class="code-cell">{old_esc}</td>
                    <td class="code-cell">{new_esc}</td>
                    <td>{f.description}</td>
                    <td>{status}</td>
                </tr>
"""
    html += f"""            </tbody>
        </table>

        <p style="margin-top:30px; font-size:12px; color:#6c757d; text-align: center;">
            Report generated by Airflow 3 Migration Assistant on {timestamp} UTC.
        </p>
    </div>
</body>
</html>"""

    if output_path:
        Path(output_path).write_text(html, encoding="utf-8")

    return html


# ---------------------------------------------------------------------------
# Console Summary
# ---------------------------------------------------------------------------

def print_console_summary(findings: list[MigrationFinding], files_scanned: int, dry_run: bool):
    """Print a concise summary to the console."""
    from collections import Counter

    total = len(findings)
    breaking = sum(1 for f in findings if f.severity == "BREAKING")
    deprecated = sum(1 for f in findings if f.severity == "DEPRECATED")
    auto_fixable = sum(1 for f in findings if f.auto_fixable)
    applied = sum(1 for f in findings if f.applied)
    files_affected = len(set(f.file for f in findings))

    print("\n" + "=" * 70)
    print("  AIRFLOW 3 MIGRATION ASSISTANT - SCAN RESULTS")
    print("=" * 70)
    print(f"  Mode:              {'DRY RUN (no changes made)' if dry_run else 'APPLY MODE'}")
    print(f"  Files scanned:     {files_scanned}")
    print(f"  Files with issues: {files_affected}")
    print(f"  Total findings:    {total}")
    print(f"  Breaking changes:  {breaking}")
    print(f"  Deprecations:      {deprecated}")
    print(f"  Auto-fixable:      {auto_fixable}")
    if not dry_run:
        print(f"  Applied fixes:     {applied}")
    print("-" * 70)

    if total == 0:
        print("  No migration issues found! Your DAGs are Airflow 3 ready.")
        print("=" * 70 + "\n")
        return

    cat_counts = Counter(f.category for f in findings)
    print("\n  Issues by category:")
    for cat, count in cat_counts.most_common():
        print(f"    {cat:30s} {count}")

    print("\n  Top files:")
    file_counts = Counter(f.file for f in findings)
    for fp, count in file_counts.most_common(10):
        print(f"    {fp:50s} {count} issues")

    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Airflow 3 Migration Assistant - Scan and convert Airflow 2 DAGs to Airflow 3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run - scan and generate report only
  python airflow3_migration_assistant.py /path/to/dags --dry-run

  # Apply fixes automatically
  python airflow3_migration_assistant.py /path/to/dags --apply

  # Scan with custom report output
  python airflow3_migration_assistant.py /path/to/dags --dry-run --report-output ./migration_report.html

  # Apply without backups (not recommended)
  python airflow3_migration_assistant.py /path/to/dags --apply --no-backup
        """,
    )
    parser.add_argument("dag_directory", help="Path to the directory containing Airflow DAG files")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Scan and report only, do not modify files (default)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply auto-fixable changes to DAG files",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Skip creating .airflow2.bak backup files when applying fixes",
    )
    parser.add_argument(
        "--report-output", "-o",
        default=None,
        help="Path to write the HTML report (default: <dag_directory>/airflow3_migration_report.html)",
    )

    args = parser.parse_args()

    dag_dir = os.path.abspath(args.dag_directory)
    if not os.path.isdir(dag_dir):
        print(f"Error: '{dag_dir}' is not a valid directory.")
        sys.exit(1)

    dry_run = not args.apply

    print(f"\nScanning DAGs in: {dag_dir}")
    print(f"Mode: {'DRY RUN' if dry_run else 'APPLY'}\n")

    # Scan
    scanner = Airflow3MigrationScanner(dag_dir, dry_run=dry_run)
    findings = scanner.scan()

    # Apply fixes if not dry-run
    if not dry_run and findings:
        fixer = Airflow3MigrationFixer(dag_dir, findings, backup=not args.no_backup)
        applied_count = fixer.apply()
        print(f"Applied {applied_count} fixes across {fixer.files_modified} files.")

    # Console summary
    print_console_summary(findings, scanner.files_scanned, dry_run)

    # HTML report
    report_path = args.report_output or os.path.join(dag_dir, "airflow3_migration_report.html")
    generate_html_report(
        dag_directory=dag_dir,
        findings=findings,
        files_scanned=scanner.files_scanned,
        files_with_issues=scanner.files_with_issues,
        dry_run=dry_run,
        output_path=report_path,
    )
    print(f"HTML report written to: {report_path}\n")


if __name__ == "__main__":
    main()
