#!/usr/bin/env python3
"""
Spark Migration Code Scanner
Scans codebases for issues when migrating from Spark 2.4 to 3.5 and HDFS to S3.

Usage:
    python spark_migration_scanner.py <path> [--output report.md] [--format md|json|html]
    python spark_migration_scanner.py /path/to/project --output migration_report.md
"""

import argparse
import json
import logging
import os
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class Severity(Enum):
    CRITICAL = "critical"  # Will break at runtime
    HIGH = "high"          # Likely to break or cause data issues
    MEDIUM = "medium"      # Behavioral changes, potential issues
    LOW = "low"            # Deprecation warnings, style issues
    INFO = "info"          # Informational, optimization opportunities


class Category(Enum):
    SPARK_API = "Spark API Changes"
    SPARK_SQL = "Spark SQL Changes"
    SPARK_CONFIG = "Configuration Changes"
    SPARK_DEPS = "Dependency Changes"
    HDFS_TO_S3 = "HDFS to S3 Migration"
    PERFORMANCE = "Performance Considerations"
    SECURITY = "Security & Auth"
    JDK_MIGRATION = "JDK 8/11 to 17 Migration"
    PYTHON_MIGRATION = "Python 2 to 3 Migration"


@dataclass
class Issue:
    rule_id: str
    title: str
    description: str
    severity: Severity
    category: Category
    file_path: str
    line_number: int
    line_content: str
    suggestion: str
    documentation_url: Optional[str] = None


@dataclass
class ScanRule:
    rule_id: str
    title: str
    description: str
    severity: Severity
    category: Category
    pattern: re.Pattern
    suggestion: str
    documentation_url: Optional[str] = None
    file_extensions: tuple = (".py", ".scala", ".java", ".sql", ".conf", ".properties", ".xml", ".yaml", ".yml")


# =============================================================================
# SPARK 2.4 -> 3.5 MIGRATION RULES
# =============================================================================

SPARK_API_RULES = [
    # Critical API Removals
    ScanRule(
        rule_id="SPARK-001",
        title="Removed: SQLContext",
        description="SQLContext was removed in Spark 3.0. Use SparkSession instead.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'\b(SQLContext|sqlContext)\b', re.IGNORECASE),
        suggestion="Replace with SparkSession: spark = SparkSession.builder.getOrCreate()",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-migration-guide.html"
    ),
    ScanRule(
        rule_id="SPARK-002",
        title="Removed: HiveContext",
        description="HiveContext was removed in Spark 3.0. Use SparkSession.builder.enableHiveSupport().",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'\bHiveContext\b'),
        suggestion="Replace with: SparkSession.builder.enableHiveSupport().getOrCreate()",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-migration-guide.html"
    ),
    ScanRule(
        rule_id="SPARK-003",
        title="Removed: DataFrame.registerTempTable",
        description="registerTempTable was removed. Use createOrReplaceTempView instead.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.registerTempTable\s*\('),
        suggestion="Replace with .createOrReplaceTempView()",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-migration-guide.html"
    ),
    ScanRule(
        rule_id="SPARK-004",
        title="Removed: DataFrame.toJSON with RDD",
        description="toJSON now returns Dataset[String] instead of RDD[String].",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.toJSON\s*\(\s*\)\.(?:map|flatMap|filter|collect)'),
        suggestion="Update code to work with Dataset[String] or call .rdd explicitly",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-migration-guide.html"
    ),
    ScanRule(
        rule_id="SPARK-005",
        title="Removed: Accumulator (old API)",
        description="The old Accumulator API was removed. Use AccumulatorV2 instead.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'sc\.accumulator\s*\(|Accumulator\['),
        suggestion="Migrate to AccumulatorV2 API",
        documentation_url="https://spark.apache.org/docs/3.5.0/rdd-programming-guide.html#accumulators"
    ),
    ScanRule(
        rule_id="SPARK-006",
        title="Changed: SparkContext.broadcast type",
        description="broadcast() return type changed. Ensure compatibility with new Broadcast[T] API.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.broadcast\s*\([^)]+\)\.value'),
        suggestion="Review broadcast variable usage for API compatibility",
    ),
    ScanRule(
        rule_id="SPARK-007",
        title="Removed: RDD.mapPartitionsWithIndex with preservesPartitioning",
        description="The preservesPartitioning parameter was removed from mapPartitionsWithIndex.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.mapPartitionsWithIndex\s*\([^)]*preservesPartitioning'),
        suggestion="Remove preservesPartitioning parameter; use mapPartitions with index if needed",
    ),
    ScanRule(
        rule_id="SPARK-008",
        title="Deprecated: RDD.toLocalIterator",
        description="toLocalIterator behavior changed; may cause memory issues with large partitions.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.toLocalIterator\s*\('),
        suggestion="Consider using .collect() for small datasets or process partitions differently",
    ),
    ScanRule(
        rule_id="SPARK-009",
        title="Changed: Dataset.unionAll renamed to union",
        description="unionAll was renamed to union in Spark 2.0 and removed in 3.0.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.unionAll\s*\('),
        suggestion="Replace .unionAll() with .union()",
    ),
    ScanRule(
        rule_id="SPARK-010",
        title="Changed: DataFrame.explode removed",
        description="DataFrame.explode was removed. Use functions.explode with select/withColumn.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.explode\s*\([^)]*\)\s*\{'),
        suggestion="Use: df.select(col('*'), explode(col('array_col')).alias('exploded'))",
    ),
    ScanRule(
        rule_id="SPARK-011",
        title="Removed: DataFrame.rdd.toJavaRDD",
        description="Direct toJavaRDD access pattern changed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.rdd\s*\.toJavaRDD'),
        suggestion="Use JavaRDD.fromRDD() or work with Scala RDD directly",
    ),
    ScanRule(
        rule_id="SPARK-012",
        title="Changed: Window function defaults",
        description="Window functions now require explicit frame specification in some cases.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'Window\.(partitionBy|orderBy)\s*\([^)]+\)(?!\s*\.rows|\s*\.range)'),
        suggestion="Add explicit frame spec: .rowsBetween(Window.unboundedPreceding, Window.currentRow)",
    ),
    ScanRule(
        rule_id="SPARK-013",
        title="Removed: DataFrameWriter.insertInto with overwrite mode",
        description="insertInto behavior changed with overwrite mode.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.mode\s*\(\s*["\']?overwrite["\']?\s*\)\s*\.insertInto'),
        suggestion="Use .mode('overwrite').saveAsTable() or explicit partition overwrite",
    ),
    ScanRule(
        rule_id="SPARK-014",
        title="Changed: UDF return type inference",
        description="UDF return type inference is stricter in Spark 3.x.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'udf\s*\(\s*lambda[^:]+:[^,)]+\)(?!\s*,)'),
        suggestion="Explicitly specify return type: udf(lambda x: x, StringType())",
    ),
    ScanRule(
        rule_id="SPARK-015",
        title="Changed: Pandas UDF decorator syntax",
        description="@pandas_udf decorator syntax changed in Spark 3.0.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'@pandas_udf\s*\(\s*["\'][^"\']+["\']\s*,\s*PandasUDFType'),
        suggestion="Use new syntax: @pandas_udf(returnType) with type hints",
        documentation_url="https://spark.apache.org/docs/3.5.0/api/python/user_guide/sql/arrow_pandas.html"
    ),
    ScanRule(
        rule_id="SPARK-016",
        title="Removed: toPandas with Arrow disabled",
        description="Arrow optimization for toPandas is now default; old behavior removed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_API,
        pattern=re.compile(r'spark\.conf\.set\s*\([^)]*arrow\.enabled[^)]*false', re.IGNORECASE),
        suggestion="Arrow is now required; ensure PyArrow is installed and compatible",
    ),

    # Structured Streaming Changes
    ScanRule(
        rule_id="SPARK-020",
        title="Changed: Structured Streaming foreach/foreachBatch",
        description="foreach and foreachBatch API signatures changed.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.foreach\s*\(\s*lambda'),
        suggestion="Use ForeachWriter class or foreachBatch with proper signature",
    ),
    ScanRule(
        rule_id="SPARK-021",
        title="Removed: Streaming DataFrame.isStreaming",
        description="Check streaming status via DataFrame.isStreaming property.",
        severity=Severity.LOW,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.isStreaming\s*\(\s*\)'),
        suggestion="Use .isStreaming (property, not method)",
    ),
    ScanRule(
        rule_id="SPARK-022",
        title="Changed: Streaming trigger syntax",
        description="Trigger.ProcessingTime syntax changed in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_API,
        pattern=re.compile(r'Trigger\.ProcessingTime\s*\(\s*["\']'),
        suggestion="Use Trigger.ProcessingTime('interval') or processingTime='interval'",
    ),

    # ML/MLlib Changes
    ScanRule(
        rule_id="SPARK-030",
        title="Removed: spark.mllib (RDD-based ML)",
        description="RDD-based MLlib (spark.mllib) is deprecated. Use spark.ml (DataFrame-based).",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'from\s+pyspark\.mllib\b|import\s+org\.apache\.spark\.mllib\b'),
        suggestion="Migrate to DataFrame-based ML: pyspark.ml / org.apache.spark.ml",
        documentation_url="https://spark.apache.org/docs/3.5.0/ml-guide.html"
    ),
    ScanRule(
        rule_id="SPARK-031",
        title="Changed: ML Pipeline persistence format",
        description="ML Pipeline model format changed; models may need retraining.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'\.load\s*\(\s*["\'][^"\']*model[^"\']*["\']\s*\)', re.IGNORECASE),
        suggestion="Retrain models with Spark 3.x or use MLflow for versioned model management",
    ),
    ScanRule(
        rule_id="SPARK-032",
        title="Removed: OneHotEncoder (old version)",
        description="Old OneHotEncoder removed; use OneHotEncoder with dropLast parameter.",
        severity=Severity.HIGH,
        category=Category.SPARK_API,
        pattern=re.compile(r'OneHotEncoderEstimator'),
        suggestion="Use OneHotEncoder (renamed from OneHotEncoderEstimator in 3.0)",
    ),
]

SPARK_SQL_RULES = [
    # SQL Function Changes
    ScanRule(
        rule_id="SQL-001",
        title="Changed: CAST behavior for invalid values",
        description="CAST now returns NULL for invalid input instead of throwing exception.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'\bCAST\s*\([^)]+\s+AS\s+(INT|INTEGER|BIGINT|DOUBLE|FLOAT|DECIMAL)', re.IGNORECASE),
        suggestion="Review CAST usage; invalid values now return NULL. Use try_cast() if needed.",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-migration-guide.html"
    ),
    ScanRule(
        rule_id="SQL-002",
        title="Changed: ANSI SQL mode default",
        description="spark.sql.ansi.enabled affects arithmetic operations and type conversions.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'spark\.sql\.ansi\.enabled'),
        suggestion="Review ANSI mode settings; overflow behavior changed in Spark 3.x",
    ),
    ScanRule(
        rule_id="SQL-003",
        title="Changed: from_json schema parameter",
        description="from_json now requires explicit schema; inference removed.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'from_json\s*\([^,]+\)(?!\s*,)'),
        suggestion="Add explicit schema: from_json(col, schema)",
    ),
    ScanRule(
        rule_id="SQL-004",
        title="Changed: Date/Timestamp parsing",
        description="Date and timestamp parsing is stricter in Spark 3.x.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'to_date\s*\([^,]+\)(?!\s*,)|to_timestamp\s*\([^,]+\)(?!\s*,)'),
        suggestion="Add explicit format: to_date(col, 'yyyy-MM-dd')",
    ),
    ScanRule(
        rule_id="SQL-005",
        title="Changed: Grouping without aggregation",
        description="SELECT with GROUP BY requires aggregation functions in Spark 3.x.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'GROUP\s+BY\s+(?!.*(?:COUNT|SUM|AVG|MIN|MAX|FIRST|LAST|COLLECT_LIST|COLLECT_SET)\s*\()', re.IGNORECASE),
        suggestion="Ensure all non-grouped columns use aggregation functions",
    ),
    ScanRule(
        rule_id="SQL-006",
        title="Removed: Hive bucketing with different sorting",
        description="Hive bucketing behavior changed; bucket sort order must match.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'bucketBy\s*\([^)]+\)\.sortBy\s*\('),
        suggestion="Ensure bucket and sort columns are consistent; review bucketing strategy",
    ),
    ScanRule(
        rule_id="SQL-007",
        title="Changed: String comparison behavior",
        description="String comparisons use different collation in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"(?:=|<>|!=|<|>|<=|>=)\s*['\"][^'\"]*['\"]", re.IGNORECASE),
        suggestion="Review string comparisons for collation-sensitive queries",
    ),
    ScanRule(
        rule_id="SQL-008",
        title="Changed: Numeric precision in division",
        description="Division precision handling changed in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'\s+/\s+(?:CAST|[0-9])', re.IGNORECASE),
        suggestion="Review division operations for precision requirements",
    ),
    ScanRule(
        rule_id="SQL-009",
        title="Changed: PERCENTILE_APPROX accuracy",
        description="PERCENTILE_APPROX algorithm changed; results may differ slightly.",
        severity=Severity.LOW,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'percentile_approx\s*\(', re.IGNORECASE),
        suggestion="Review percentile calculations; consider explicit accuracy parameter",
    ),
    ScanRule(
        rule_id="SQL-010",
        title="Changed: Sequence function parameters",
        description="sequence() function parameter validation is stricter.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'sequence\s*\(', re.IGNORECASE),
        suggestion="Ensure sequence start, stop, step parameters are valid",
    ),
    ScanRule(
        rule_id="SQL-011",
        title="Deprecated: HiveQL syntax",
        description="Some HiveQL-specific syntax deprecated in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'SORT\s+BY|DISTRIBUTE\s+BY|CLUSTER\s+BY', re.IGNORECASE),
        suggestion="Use ORDER BY with window functions or DataFrame API for explicit control",
    ),
    ScanRule(
        rule_id="SQL-012",
        title="Changed: CREATE TABLE data source",
        description="CREATE TABLE without USING clause defaults changed.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?!.*USING)', re.IGNORECASE),
        suggestion="Explicitly specify data source: CREATE TABLE ... USING parquet/delta/iceberg",
    ),
    ScanRule(
        rule_id="SQL-013",
        title="Changed: INSERT OVERWRITE partition behavior",
        description="INSERT OVERWRITE with dynamic partitions behavior changed.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'INSERT\s+OVERWRITE\s+(?:TABLE\s+)?\w+\s+PARTITION', re.IGNORECASE),
        suggestion="Review spark.sql.sources.partitionOverwriteMode setting (dynamic vs static)",
    ),
    ScanRule(
        rule_id="SQL-014",
        title="Changed: COALESCE behavior with nulls",
        description="COALESCE evaluation order and null handling refined.",
        severity=Severity.LOW,
        category=Category.SPARK_SQL,
        pattern=re.compile(r'COALESCE\s*\(', re.IGNORECASE),
        suggestion="Review COALESCE usage with complex expressions",
    ),
    ScanRule(
        rule_id="SQL-015",
        title="Removed: Legacy datetime formats",
        description="Legacy datetime pattern letters removed (e.g., 'Y' vs 'y').",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"date_format\s*\([^)]*['\"][^'\"]*[YDHMS][^'\"]*['\"]", re.IGNORECASE),
        suggestion="Use Java 8 datetime patterns: 'yyyy-MM-dd' not 'YYYY-MM-DD'",
    ),

    # ==========================================================================
    # HIVE SET COMMANDS AND PROPERTIES TO REMOVE
    # ==========================================================================

    # Hive Execution Engine Properties
    ScanRule(
        rule_id="HIVE-001",
        title="Remove: SET hive.execution.engine",
        description="hive.execution.engine is not applicable in Spark SQL.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.execution\.engine\s*=", re.IGNORECASE),
        suggestion="Remove this SET command; Spark uses its own execution engine",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-002",
        title="Remove: SET hive.exec.* properties",
        description="hive.exec.* properties are Hive-specific and should be removed.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.exec\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.exec.* properties; use equivalent Spark configs (spark.sql.*)",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-003",
        title="Remove: SET hive.mapred.* properties",
        description="hive.mapred.* properties are not used by Spark.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.mapred\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.mapred.* properties; Spark does not use MapReduce",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-004",
        title="Remove: SET hive.tez.* properties",
        description="hive.tez.* properties are Tez-specific and not applicable to Spark.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.tez\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.tez.* properties; Spark does not use Tez",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-005",
        title="Remove: SET hive.vectorized.* properties",
        description="hive.vectorized.* properties are Hive-specific vectorization settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.vectorized\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.vectorized.* properties; Spark has its own vectorization (spark.sql.columnVector.*)",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-006",
        title="Remove: SET hive.cbo.* properties",
        description="hive.cbo.* (Cost-Based Optimizer) properties are Hive-specific.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.cbo\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.cbo.* properties; use spark.sql.cbo.* for Spark's CBO",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-007",
        title="Remove: SET hive.stats.* properties",
        description="hive.stats.* properties are Hive-specific statistics settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.stats\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.stats.* properties; use ANALYZE TABLE for Spark statistics",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-008",
        title="Remove: SET hive.optimize.* properties",
        description="hive.optimize.* properties are Hive optimizer settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.optimize\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.optimize.* properties; use spark.sql.* optimizer configs",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-009",
        title="Remove: SET hive.merge.* properties",
        description="hive.merge.* properties control Hive file merging behavior.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.merge\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.merge.* properties; use coalesce() or repartition() in Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-010",
        title="Remove: SET hive.auto.convert.join*",
        description="hive.auto.convert.join* properties are Hive join optimization settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.auto\.convert\.join[a-zA-Z0-9_.]*\s*=", re.IGNORECASE),
        suggestion="Remove; use spark.sql.autoBroadcastJoinThreshold for Spark broadcast joins",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-011",
        title="Remove: SET hive.map.aggr*",
        description="hive.map.aggr* properties control Hive map-side aggregation.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.map\.aggr[a-zA-Z0-9_.]*\s*=", re.IGNORECASE),
        suggestion="Remove; Spark handles aggregation optimization automatically",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-012",
        title="Remove: SET hive.groupby.* properties",
        description="hive.groupby.* properties are Hive GROUP BY optimization settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.groupby\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.groupby.* properties; Spark handles GROUP BY differently",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-013",
        title="Remove: SET hive.skewjoin.* properties",
        description="hive.skewjoin.* properties are Hive skew join settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.skewjoin\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use spark.sql.adaptive.skewJoin.* for Spark 3.x skew handling",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-014",
        title="Remove: SET hive.input.format",
        description="hive.input.format is a Hive-specific input format setting.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.input\.format\s*=", re.IGNORECASE),
        suggestion="Remove; Spark uses its own input format handling",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-015",
        title="Remove: SET hive.fetch.task.*",
        description="hive.fetch.task.* properties control Hive fetch task behavior.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.fetch\.task\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.fetch.task.* properties; not applicable to Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-016",
        title="Remove: SET hive.compute.query.using.stats",
        description="hive.compute.query.using.stats is a Hive statistics setting.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.compute\.query\.using\.stats\s*=", re.IGNORECASE),
        suggestion="Remove; use spark.sql.cbo.enabled for Spark statistics-based optimization",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-017",
        title="Remove: SET hive.support.concurrency",
        description="hive.support.concurrency is a Hive locking/concurrency setting.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.support\.concurrency\s*=", re.IGNORECASE),
        suggestion="Remove; Spark has different concurrency handling",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-018",
        title="Remove: SET hive.txn.* properties",
        description="hive.txn.* properties are Hive ACID transaction settings.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.txn\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use Delta Lake or Iceberg for ACID transactions in Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-019",
        title="Remove: SET hive.compactor.* properties",
        description="hive.compactor.* properties are Hive compaction settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.compactor\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use Delta Lake OPTIMIZE or Iceberg compaction",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-020",
        title="Remove: SET hive.enforce.* properties",
        description="hive.enforce.* properties are Hive enforcement settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.enforce\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.enforce.* properties; review Spark equivalents",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-021",
        title="Remove: SET hive.server2.* properties",
        description="hive.server2.* properties are HiveServer2 settings.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.server2\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use Spark Thrift Server configs (spark.sql.thriftServer.*)",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-022",
        title="Remove: SET hive.metastore.* properties",
        description="hive.metastore.* properties are metastore-specific settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.metastore\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Review; some may be needed for metastore connection, others should be removed",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-023",
        title="Remove: SET hive.session.* properties",
        description="hive.session.* properties are Hive session settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.session\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.session.* properties; not applicable to Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-024",
        title="Remove: SET hive.llap.* properties",
        description="hive.llap.* properties are Hive LLAP (Live Long And Process) settings.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.llap\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.llap.* properties; LLAP is not used by Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-025",
        title="Remove: SET hive.prewarm.* properties",
        description="hive.prewarm.* properties are Hive container prewarm settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.prewarm\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use Spark dynamic allocation settings instead",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-026",
        title="Remove: SET hive.smbjoin.* properties",
        description="hive.smbjoin.* properties are Hive sort-merge-bucket join settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.smbjoin\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use Spark's built-in join optimization",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-027",
        title="Remove: SET hive.limit.* properties",
        description="hive.limit.* properties control Hive LIMIT optimization.",
        severity=Severity.LOW,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.limit\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; Spark handles LIMIT optimization automatically",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-028",
        title="Remove: SET hive.hashtable.* properties",
        description="hive.hashtable.* properties are Hive hash table settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.hashtable\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; Spark manages hash tables internally",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-029",
        title="Remove: SET hive.query.* properties",
        description="hive.query.* properties are Hive query execution settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.query\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; use spark.sql.* for query configuration",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-030",
        title="Remove: SET hive.resultset.* properties",
        description="hive.resultset.* properties are Hive result set settings.",
        severity=Severity.LOW,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.resultset\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove hive.resultset.* properties; not applicable to Spark",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),

    # Generic Hive SET pattern (catch-all)
    ScanRule(
        rule_id="HIVE-099",
        title="Review: SET hive.* property detected",
        description="Generic hive.* property detected; review for Spark compatibility.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+hive\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Review and remove Hive-specific properties; migrate to Spark equivalents",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),

    # MapReduce SET properties (often in legacy Hive scripts)
    ScanRule(
        rule_id="HIVE-100",
        title="Remove: SET mapred.* properties",
        description="mapred.* properties are MapReduce settings not used by Spark.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+mapred\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove mapred.* properties; Spark does not use MapReduce",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-101",
        title="Remove: SET mapreduce.* properties",
        description="mapreduce.* properties are MapReduce settings not used by Spark.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+mapreduce\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove mapreduce.* properties; Spark does not use MapReduce",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-102",
        title="Remove: SET yarn.* properties in SQL",
        description="yarn.* properties should not be SET in SQL scripts.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+yarn\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove; configure YARN settings in spark-defaults.conf or submit command",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-103",
        title="Remove: SET tez.* properties",
        description="tez.* properties are Tez execution engine settings.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"SET\s+tez\.[a-zA-Z0-9_.]+\s*=", re.IGNORECASE),
        suggestion="Remove tez.* properties; Spark does not use Tez",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),

    # Additional Hive-specific SQL patterns
    ScanRule(
        rule_id="HIVE-110",
        title="Review: ADD JAR command",
        description="ADD JAR is Hive-specific; use --jars or spark.jars config.",
        severity=Severity.HIGH,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"ADD\s+JAR\s+", re.IGNORECASE),
        suggestion="Remove ADD JAR; use --jars in spark-submit or spark.jars config",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-111",
        title="Review: ADD FILE command",
        description="ADD FILE is Hive-specific; use --files or spark.files config.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"ADD\s+FILE\s+", re.IGNORECASE),
        suggestion="Remove ADD FILE; use --files in spark-submit or spark.files config",
        file_extensions=(".sql", ".hql", ".py", ".scala", ".java")
    ),
    ScanRule(
        rule_id="HIVE-112",
        title="Review: dfs command in SQL",
        description="dfs commands are Hive CLI specific.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"^\s*dfs\s+", re.IGNORECASE | re.MULTILINE),
        suggestion="Remove dfs commands; use Spark file APIs or shell scripts",
        file_extensions=(".sql", ".hql")
    ),
    ScanRule(
        rule_id="HIVE-113",
        title="Review: source command in SQL",
        description="source command is Hive CLI specific.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_SQL,
        pattern=re.compile(r"^\s*source\s+", re.IGNORECASE | re.MULTILINE),
        suggestion="Remove source command; execute SQL files directly in Spark",
        file_extensions=(".sql", ".hql")
    ),
]

SPARK_CONFIG_RULES = [
    # Configuration Changes
    ScanRule(
        rule_id="CFG-001",
        title="Renamed: spark.sql.hive.convertMetastoreParquet",
        description="Hive metastore conversion configs renamed/changed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.hive\.convertMetastoreParquet'),
        suggestion="Review Hive metastore integration settings for Spark 3.x",
    ),
    ScanRule(
        rule_id="CFG-002",
        title="Changed: spark.sql.shuffle.partitions default",
        description="Consider adjusting shuffle partitions for performance.",
        severity=Severity.LOW,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.shuffle\.partitions\s*[=:]\s*200'),
        suggestion="Consider spark.sql.adaptive.enabled=true for dynamic partition tuning",
    ),
    ScanRule(
        rule_id="CFG-003",
        title="New: Adaptive Query Execution (AQE)",
        description="AQE is available and recommended in Spark 3.x.",
        severity=Severity.INFO,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.adaptive\.enabled\s*[=:]\s*false', re.IGNORECASE),
        suggestion="Enable AQE: spark.sql.adaptive.enabled=true for better performance",
        documentation_url="https://spark.apache.org/docs/3.5.0/sql-performance-tuning.html#adaptive-query-execution"
    ),
    ScanRule(
        rule_id="CFG-004",
        title="Removed: spark.sql.execution.arrow.enabled",
        description="Config renamed to spark.sql.execution.arrow.pyspark.enabled.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.execution\.arrow\.enabled(?!\.pyspark)'),
        suggestion="Use spark.sql.execution.arrow.pyspark.enabled",
    ),
    ScanRule(
        rule_id="CFG-005",
        title="Changed: spark.sql.legacy.timeParserPolicy",
        description="Datetime parsing policy defaults changed in Spark 3.x.",
        severity=Severity.HIGH,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.legacy\.timeParserPolicy'),
        suggestion="Review datetime parsing; set to LEGACY for backward compatibility if needed",
    ),
    ScanRule(
        rule_id="CFG-006",
        title="Removed: spark.sql.parquet.int96AsTimestamp",
        description="INT96 timestamp handling changed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.parquet\.int96AsTimestamp'),
        suggestion="Use spark.sql.parquet.outputTimestampType for explicit control",
    ),
    ScanRule(
        rule_id="CFG-007",
        title="Changed: spark.executor.extraJavaOptions format",
        description="Java options format requirements changed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.executor\.extraJavaOptions\s*[=:]'),
        suggestion="Review Java options; some JVM flags changed in Java 11+",
    ),
    ScanRule(
        rule_id="CFG-008",
        title="New: Kubernetes executor configs",
        description="Review Kubernetes-specific configurations for Spark 3.x.",
        severity=Severity.INFO,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.kubernetes\.'),
        suggestion="Spark 3.x has improved K8s support; review new config options",
    ),
    ScanRule(
        rule_id="CFG-009",
        title="Changed: Dynamic allocation configs",
        description="Dynamic allocation behavior and configs changed.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.dynamicAllocation\.'),
        suggestion="Review dynamic allocation settings for Spark 3.x compatibility",
    ),
    ScanRule(
        rule_id="CFG-010",
        title="Removed: spark.sql.orc.impl",
        description="ORC implementation selection removed; native is default.",
        severity=Severity.LOW,
        category=Category.SPARK_CONFIG,
        pattern=re.compile(r'spark\.sql\.orc\.impl'),
        suggestion="Remove this config; native ORC implementation is now default",
    ),
]

SPARK_DEPENDENCY_RULES = [
    # Dependency Changes
    ScanRule(
        rule_id="DEP-001",
        title="Changed: Scala version (2.11 -> 2.12/2.13)",
        description="Spark 3.x requires Scala 2.12 or 2.13; Scala 2.11 not supported.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'scala-library[_-]2\.11|scalaVersion\s*:?=?\s*["\']?2\.11'),
        suggestion="Upgrade to Scala 2.12.x or 2.13.x",
        documentation_url="https://spark.apache.org/docs/3.5.0/"
    ),
    ScanRule(
        rule_id="DEP-002",
        title="Changed: Java version (8 -> 8/11/17)",
        description="Spark 3.x supports Java 8, 11, and 17; review JVM settings.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'source\s*[=:]\s*["\']?1\.[78]["\']?|java\.version.*1\.[78]'),
        suggestion="Consider upgrading to Java 11 or 17 for better performance",
    ),
    ScanRule(
        rule_id="DEP-003",
        title="Removed: spark-avro built-in",
        description="spark-avro module changes in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'com\.databricks.*spark-avro|spark-avro_2\.11'),
        suggestion="Use org.apache.spark:spark-avro_2.12 (built into Spark 3.x)",
    ),
    ScanRule(
        rule_id="DEP-004",
        title="Changed: Hive dependency version",
        description="Hive metastore version compatibility changed in Spark 3.x.",
        severity=Severity.HIGH,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'hive-metastore.*[12]\.[0-3]\.|spark\.sql\.hive\.metastore\.version.*[12]\.[0-3]'),
        suggestion="Upgrade Hive metastore or use Spark's built-in Hive support",
    ),
    ScanRule(
        rule_id="DEP-005",
        title="Changed: Hadoop version (2.x -> 3.x)",
        description="Spark 3.x uses Hadoop 3.x by default.",
        severity=Severity.HIGH,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'hadoop.*2\.[0-9]\.|org\.apache\.hadoop:hadoop-(core|common|client):2\.[0-9]'),
        suggestion="Upgrade to Hadoop 3.x compatible dependencies",
    ),
    ScanRule(
        rule_id="DEP-006",
        title="Changed: Python version (2.7 -> 3.6+)",
        description="PySpark 3.x requires Python 3.6+; Python 2.7 not supported.",
        severity=Severity.CRITICAL,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'python_requires.*[<>=]=?.*2\.[67]|python2\.7|#!/.*python2'),
        suggestion="Upgrade to Python 3.8+ for best compatibility",
    ),
    ScanRule(
        rule_id="DEP-007",
        title="Changed: PyArrow compatibility",
        description="PyArrow version requirements changed in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'pyarrow[<>=]=?[0-3]\.'),
        suggestion="Upgrade PyArrow to 4.0+ for Spark 3.x compatibility",
    ),
    ScanRule(
        rule_id="DEP-008",
        title="Changed: Pandas version",
        description="Pandas version requirements changed; API changes in newer versions.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'pandas[<>=]=?0\.[12]'),
        suggestion="Upgrade Pandas to 1.0+ for Spark 3.x compatibility",
    ),
    ScanRule(
        rule_id="DEP-009",
        title="Removed: Jackson 2.9.x support",
        description="Jackson library version upgraded in Spark 3.x.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'jackson.*2\.[0-9]\.'),
        suggestion="Upgrade Jackson to 2.12+ to match Spark 3.x",
    ),
    ScanRule(
        rule_id="DEP-010",
        title="Changed: spark-submit packages",
        description="Review --packages arguments for version compatibility.",
        severity=Severity.MEDIUM,
        category=Category.SPARK_DEPS,
        pattern=re.compile(r'--packages[^&|;]*2\.4|spark-packages.*2\.4'),
        suggestion="Update package versions for Spark 3.x compatibility",
    ),
]

# =============================================================================
# HDFS TO S3 MIGRATION RULES
# =============================================================================

HDFS_TO_S3_RULES = [
    # Path and URI Changes
    ScanRule(
        rule_id="S3-001",
        title="HDFS Path: Requires S3 conversion",
        description="HDFS paths need to be converted to S3 paths.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'hdfs://[^\s"\')]+'),
        suggestion="Replace hdfs://namenode:port/path with s3a://bucket/path",
    ),
    ScanRule(
        rule_id="S3-002",
        title="HDFS Path: Relative path",
        description="Relative HDFS paths need S3 bucket prefix.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'(?:\.load|\.save|\.parquet|\.csv|\.json|\.orc)\s*\(\s*["\'](?!/|hdfs|s3|gs|abfs|wasb)[^"\']+["\']'),
        suggestion="Add explicit S3 path: s3a://bucket/path/data",
    ),
    ScanRule(
        rule_id="S3-003",
        title="Deprecated: s3n/s3 protocol",
        description="Use s3a:// protocol instead of deprecated s3:// or s3n://.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\bs3n?://[^\s"\')]+'),
        suggestion="Use s3a:// protocol for better performance and features",
    ),
    ScanRule(
        rule_id="S3-004",
        title="HDFS API: FileSystem usage",
        description="Direct FileSystem API calls need S3 adapter.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'FileSystem\.get\s*\(|hadoop\.fs\.FileSystem'),
        suggestion="Use S3AFileSystem or abstract file operations",
    ),
    ScanRule(
        rule_id="S3-005",
        title="HDFS API: Path hardcoding",
        description="Hardcoded /user/ or /tmp/ paths won't work on S3.",
        severity=Severity.MEDIUM,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'["\'](?:/user/|/tmp/|/data/|/warehouse/)[^"\']*["\']'),
        suggestion="Use configurable base paths: s3a://bucket/user/, s3a://bucket/tmp/",
    ),

    # S3-Specific Configurations
    ScanRule(
        rule_id="S3-010",
        title="Required: S3A credentials configuration",
        description="S3 access requires credential configuration.",
        severity=Severity.INFO,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'fs\.s3a\.'),
        suggestion="Ensure IAM roles or access keys are configured properly",
    ),
    ScanRule(
        rule_id="S3-011",
        title="Recommended: S3A committer configuration",
        description="Use S3A committers for reliable output.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.save\s*\(|\.write\.|saveAsTextFile|saveAsParquet'),
        suggestion="Configure spark.hadoop.fs.s3a.committer.name=magic or directory",
        documentation_url="https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html"
    ),
    ScanRule(
        rule_id="S3-012",
        title="HDFS-specific: replication factor",
        description="Replication factor is not applicable to S3.",
        severity=Severity.LOW,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'dfs\.replication|setReplication'),
        suggestion="Remove HDFS replication settings; S3 handles durability differently",
    ),
    ScanRule(
        rule_id="S3-013",
        title="HDFS-specific: block size configuration",
        description="HDFS block size configs don't apply to S3.",
        severity=Severity.LOW,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'dfs\.blocksize|dfs\.block\.size'),
        suggestion="Remove HDFS block size configs; use fs.s3a.block.size if needed",
    ),

    # S3 Performance Considerations
    ScanRule(
        rule_id="S3-020",
        title="Performance: Small file writes",
        description="Small file writes are expensive on S3.",
        severity=Severity.MEDIUM,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.repartition\s*\(\s*[1-9]\s*\)|\.coalesce\s*\(\s*1\s*\)'),
        suggestion="Avoid single-file writes; use coalesce with reasonable partition count",
    ),
    ScanRule(
        rule_id="S3-021",
        title="Performance: Listing operations",
        description="Directory listings are expensive on S3.",
        severity=Severity.MEDIUM,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'listStatus|listFiles|globStatus'),
        suggestion="Minimize list operations; use partition pruning and explicit paths",
    ),
    ScanRule(
        rule_id="S3-022",
        title="Performance: Rename operations",
        description="Rename is copy+delete on S3 (expensive).",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.rename\s*\(|FileUtil\.rename'),
        suggestion="Avoid renames; use S3A committers or write directly to final location",
    ),
    ScanRule(
        rule_id="S3-023",
        title="Performance: Append operations",
        description="S3 doesn't support append; requires full rewrite.",
        severity=Severity.HIGH,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.mode\s*\(\s*["\']append["\']|SaveMode\.Append'),
        suggestion="Use partitioned writes or consider Delta Lake/Iceberg for append patterns",
    ),
    ScanRule(
        rule_id="S3-024",
        title="Performance: Speculative execution",
        description="Speculative execution can cause duplicate writes to S3.",
        severity=Severity.MEDIUM,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'spark\.speculation\s*[=:]\s*true'),
        suggestion="Disable speculation or use idempotent committers with S3",
    ),

    # S3 Consistency and Transactions
    ScanRule(
        rule_id="S3-030",
        title="Consistency: S3 eventual consistency handling",
        description="S3 strong consistency (since Dec 2020) simplifies handling.",
        severity=Severity.INFO,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'fs\.s3a\.consistent|s3guard|s3-consistent'),
        suggestion="S3Guard/S3-Consistent no longer needed; S3 is now strongly consistent",
    ),
    ScanRule(
        rule_id="S3-031",
        title="Consistency: Check-then-write patterns",
        description="Check-then-write is not atomic on S3.",
        severity=Severity.MEDIUM,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.exists\s*\([^)]+\)\s*(?:&&|\|\||if|then|and|or|and then)'),
        suggestion="Use conditional writes or external locking for atomic operations",
    ),

    # Table Format Recommendations
    ScanRule(
        rule_id="S3-040",
        title="Recommended: Use table formats for S3",
        description="Consider Delta Lake, Iceberg, or Hudi for S3 workloads.",
        severity=Severity.INFO,
        category=Category.HDFS_TO_S3,
        pattern=re.compile(r'\.parquet\s*\(|\.format\s*\(\s*["\']parquet["\']'),
        suggestion="Consider Delta Lake, Iceberg, or Hudi for ACID transactions on S3",
        documentation_url="https://iceberg.apache.org/docs/latest/"
    ),

    # Security and Auth
    ScanRule(
        rule_id="S3-050",
        title="Security: Hardcoded AWS credentials",
        description="Hardcoded credentials are a security risk.",
        severity=Severity.CRITICAL,
        category=Category.SECURITY,
        pattern=re.compile(r'(?:AKIA|aws_access_key_id|fs\.s3a\.access\.key)[^=]*[=:]\s*["\']?[A-Z0-9]{20}'),
        suggestion="Use IAM roles, instance profiles, or external secret management",
    ),
    ScanRule(
        rule_id="S3-051",
        title="Security: Secret key in config",
        description="Secret keys in config files are a security risk.",
        severity=Severity.CRITICAL,
        category=Category.SECURITY,
        pattern=re.compile(r'(?:aws_secret_access_key|fs\.s3a\.secret\.key)[^=]*[=:]\s*["\']?[A-Za-z0-9/+=]{40}'),
        suggestion="Use IAM roles or AWS Secrets Manager for credential management",
    ),
    ScanRule(
        rule_id="S3-052",
        title="Security: Encryption configuration",
        description="Enable S3 server-side encryption.",
        severity=Severity.MEDIUM,
        category=Category.SECURITY,
        pattern=re.compile(r'fs\.s3a\.server-side-encryption'),
        suggestion="Ensure fs.s3a.server-side-encryption-algorithm is set (AES256 or SSE-KMS)",
    ),
]

PERFORMANCE_RULES = [
    # General Performance Patterns
    ScanRule(
        rule_id="PERF-001",
        title="Performance: collect() on large datasets",
        description="collect() brings all data to driver; risky for large datasets.",
        severity=Severity.MEDIUM,
        category=Category.PERFORMANCE,
        pattern=re.compile(r'\.collect\s*\(\s*\)'),
        suggestion="Use take(), head(), or write to storage instead of collect()",
    ),
    ScanRule(
        rule_id="PERF-002",
        title="Performance: Cartesian join detected",
        description="Cartesian/cross joins can cause performance issues.",
        severity=Severity.HIGH,
        category=Category.PERFORMANCE,
        pattern=re.compile(r'\.crossJoin\s*\(|CROSS\s+JOIN', re.IGNORECASE),
        suggestion="Verify cartesian join is intentional; add join conditions if possible",
    ),
    ScanRule(
        rule_id="PERF-003",
        title="Performance: UDF usage",
        description="Python UDFs are slower than native Spark functions.",
        severity=Severity.MEDIUM,
        category=Category.PERFORMANCE,
        pattern=re.compile(r'@udf|udf\s*\(|spark\.udf\.register'),
        suggestion="Consider using built-in functions or Pandas UDFs for better performance",
    ),
    ScanRule(
        rule_id="PERF-004",
        title="Performance: toPandas() on large data",
        description="toPandas() brings all data to driver memory.",
        severity=Severity.MEDIUM,
        category=Category.PERFORMANCE,
        pattern=re.compile(r'\.toPandas\s*\(\s*\)'),
        suggestion="Use spark.sql.execution.arrow.pyspark.enabled=true and limit data size",
    ),
    ScanRule(
        rule_id="PERF-005",
        title="Performance: Non-bucketed shuffle join",
        description="Large table joins may benefit from bucketing.",
        severity=Severity.INFO,
        category=Category.PERFORMANCE,
        pattern=re.compile(r'\.join\s*\([^)]+,\s*["\'][^"\']+["\']'),
        suggestion="Consider bucketing tables for frequently joined columns",
    ),
]

# =============================================================================
# JDK 8/11 -> 17 MIGRATION RULES
# =============================================================================

JDK_MIGRATION_RULES = [
    # Removed/Encapsulated APIs
    ScanRule(
        rule_id="JDK-001",
        title="Removed: sun.misc.Unsafe direct access",
        description="Direct access to sun.misc.Unsafe is restricted in JDK 17.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'sun\.misc\.Unsafe|import\s+sun\.misc\.Unsafe'),
        suggestion="Use VarHandle or MethodHandles for low-level operations; add --add-opens if absolutely required",
        documentation_url="https://openjdk.org/jeps/403"
    ),
    ScanRule(
        rule_id="JDK-002",
        title="Removed: sun.misc.BASE64Encoder/Decoder",
        description="sun.misc.BASE64Encoder/Decoder removed in JDK 9+.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'sun\.misc\.BASE64(?:Encoder|Decoder)|import\s+sun\.misc\.BASE64'),
        suggestion="Use java.util.Base64.getEncoder()/getDecoder()",
    ),
    ScanRule(
        rule_id="JDK-003",
        title="Removed: javax.xml.bind (JAXB)",
        description="JAXB was removed from JDK 11+. Add external dependency.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'javax\.xml\.bind\.|import\s+javax\.xml\.bind'),
        suggestion="Add jakarta.xml.bind-api and jaxb-runtime dependencies",
        documentation_url="https://openjdk.org/jeps/320"
    ),
    ScanRule(
        rule_id="JDK-004",
        title="Removed: javax.activation",
        description="JavaBeans Activation Framework removed from JDK 11+.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'javax\.activation\.|import\s+javax\.activation'),
        suggestion="Add jakarta.activation dependency",
    ),
    ScanRule(
        rule_id="JDK-005",
        title="Removed: javax.annotation",
        description="Common annotations (JSR 250) removed from JDK 11+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'javax\.annotation\.(?:Generated|PostConstruct|PreDestroy|Resource)'),
        suggestion="Add jakarta.annotation-api dependency",
    ),
    ScanRule(
        rule_id="JDK-006",
        title="Removed: javax.xml.ws (JAX-WS)",
        description="JAX-WS was removed from JDK 11+.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'javax\.xml\.ws\.|import\s+javax\.xml\.ws'),
        suggestion="Add jakarta.xml.ws-api and jaxws-rt dependencies",
    ),
    ScanRule(
        rule_id="JDK-007",
        title="Removed: javax.jws",
        description="Web Services metadata annotations removed from JDK 11+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'javax\.jws\.|import\s+javax\.jws'),
        suggestion="Add jakarta.jws-api dependency",
    ),
    ScanRule(
        rule_id="JDK-008",
        title="Removed: CORBA (javax.corba)",
        description="CORBA module removed from JDK 11+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'org\.omg\.|javax\.rmi\.CORBA|import\s+org\.omg\.'),
        suggestion="Use alternative RPC mechanisms (gRPC, REST); CORBA is obsolete",
    ),
    ScanRule(
        rule_id="JDK-009",
        title="Removed: java.se.ee aggregator module",
        description="Java EE modules removed from JDK 11+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'--add-modules\s+java\.se\.ee'),
        suggestion="Add individual Jakarta EE dependencies instead of using --add-modules",
    ),
    ScanRule(
        rule_id="JDK-010",
        title="Removed: Nashorn JavaScript Engine",
        description="Nashorn JavaScript engine removed in JDK 15+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'ScriptEngineManager.*nashorn|getEngineByName\s*\(\s*["\'](?:nashorn|javascript)["\']', re.IGNORECASE),
        suggestion="Use GraalJS or another JavaScript engine",
        documentation_url="https://openjdk.org/jeps/372"
    ),
    ScanRule(
        rule_id="JDK-011",
        title="Removed: Pack200 Tools",
        description="Pack200 compression tools removed in JDK 14+.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'java\.util\.jar\.Pack200|pack200|unpack200'),
        suggestion="Use jlink for custom runtime images or standard compression",
        documentation_url="https://openjdk.org/jeps/367"
    ),
    ScanRule(
        rule_id="JDK-012",
        title="Removed: RMI Activation",
        description="RMI Activation mechanism removed in JDK 17.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'java\.rmi\.activation\.|Activatable|ActivationGroup'),
        suggestion="Use alternative activation mechanisms; RMI Activation is obsolete",
        documentation_url="https://openjdk.org/jeps/407"
    ),
    ScanRule(
        rule_id="JDK-013",
        title="Removed: Applet API",
        description="Applet API deprecated for removal; removed in JDK 17+.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'java\.applet\.|extends\s+Applet|import\s+java\.applet'),
        suggestion="Migrate to Java Web Start alternatives or web technologies",
        documentation_url="https://openjdk.org/jeps/398"
    ),
    ScanRule(
        rule_id="JDK-014",
        title="Removed: Security Manager",
        description="Security Manager deprecated for removal in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'System\.setSecurityManager|SecurityManager|\.policy\s*=|java\.security\.manager'),
        suggestion="Use container-based security, OS-level sandboxing, or module system",
        documentation_url="https://openjdk.org/jeps/411"
    ),

    # Strong Encapsulation Issues
    ScanRule(
        rule_id="JDK-020",
        title="Encapsulated: sun.* internal APIs",
        description="sun.* packages are strongly encapsulated in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'import\s+sun\.(?!misc\.Unsafe)'),
        suggestion="Use supported APIs or add --add-opens/--add-exports JVM flags",
    ),
    ScanRule(
        rule_id="JDK-021",
        title="Encapsulated: com.sun.* internal APIs",
        description="com.sun.* packages are strongly encapsulated in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'import\s+com\.sun\.(?!proxy)'),
        suggestion="Use supported APIs; check for public alternatives",
    ),
    ScanRule(
        rule_id="JDK-022",
        title="Encapsulated: jdk.internal APIs",
        description="jdk.internal.* packages are not accessible in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'jdk\.internal\.|import\s+jdk\.internal'),
        suggestion="Use public API alternatives",
    ),
    ScanRule(
        rule_id="JDK-023",
        title="Reflection: setAccessible on JDK internals",
        description="setAccessible(true) on JDK internal classes fails in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'\.setAccessible\s*\(\s*true\s*\)'),
        suggestion="Avoid reflective access to internals; use --add-opens if required",
    ),
    ScanRule(
        rule_id="JDK-024",
        title="Reflection: Illegal reflective access",
        description="Deep reflection on platform classes restricted in JDK 17.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'getDeclaredField|getDeclaredMethod|getDeclaredConstructor'),
        suggestion="Review reflective access; may need --add-opens for platform classes",
    ),

    # JVM Flags and Options
    ScanRule(
        rule_id="JDK-030",
        title="Removed: -XX:+UseConcMarkSweepGC (CMS)",
        description="CMS garbage collector removed in JDK 14.",
        severity=Severity.CRITICAL,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+UseConcMarkSweepGC|UseCMSInitiatingOccupancyOnly|CMSInitiatingOccupancyFraction'),
        suggestion="Use G1GC (-XX:+UseG1GC) or ZGC (-XX:+UseZGC) instead",
        documentation_url="https://openjdk.org/jeps/363"
    ),
    ScanRule(
        rule_id="JDK-031",
        title="Removed: -XX:+UseParallelOldGC",
        description="ParallelOldGC flag removed; Parallel GC uses it by default.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+UseParallelOldGC'),
        suggestion="Remove flag; use -XX:+UseParallelGC if needed",
    ),
    ScanRule(
        rule_id="JDK-032",
        title="Removed: -XX:+AggressiveOpts",
        description="AggressiveOpts flag removed in JDK 11.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+AggressiveOpts'),
        suggestion="Remove flag; optimizations are enabled by default",
    ),
    ScanRule(
        rule_id="JDK-033",
        title="Removed: -Xincgc",
        description="Incremental GC flag removed.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-Xincgc'),
        suggestion="Remove flag; use modern GC options",
    ),
    ScanRule(
        rule_id="JDK-034",
        title="Removed: -XX:MaxPermSize",
        description="PermGen space removed in JDK 8; MaxPermSize is obsolete.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:(?:Max)?PermSize'),
        suggestion="Use -XX:MaxMetaspaceSize instead for JDK 8+",
    ),
    ScanRule(
        rule_id="JDK-035",
        title="Deprecated: -XX:+PrintGCDetails",
        description="Legacy GC logging flags deprecated; use Unified Logging.",
        severity=Severity.LOW,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+Print(?:GC|GCDetails|GCTimeStamps|GCDateStamps)'),
        suggestion="Use -Xlog:gc* for unified GC logging in JDK 9+",
        documentation_url="https://openjdk.org/jeps/271"
    ),
    ScanRule(
        rule_id="JDK-036",
        title="Changed: -XX:+UseContainerSupport",
        description="Container support enabled by default in JDK 10+.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+UseContainerSupport'),
        suggestion="Flag is enabled by default; can be removed unless explicitly disabling",
    ),
    ScanRule(
        rule_id="JDK-037",
        title="Removed: -XX:+TraceClassLoading",
        description="Class loading trace flags replaced with Unified Logging.",
        severity=Severity.LOW,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+Trace(?:ClassLoading|ClassUnloading|ClassLoadingPreorder)'),
        suggestion="Use -Xlog:class+load=info for class loading trace",
    ),
    ScanRule(
        rule_id="JDK-038",
        title="Removed: -Djava.endorsed.dirs",
        description="Endorsed directories mechanism removed in JDK 9.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-Djava\.endorsed\.dirs'),
        suggestion="Use --upgrade-module-path or module system",
    ),
    ScanRule(
        rule_id="JDK-039",
        title="Removed: -Djava.ext.dirs",
        description="Extension directories mechanism removed in JDK 9.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-Djava\.ext\.dirs'),
        suggestion="Add JARs to classpath or use module system",
    ),
    ScanRule(
        rule_id="JDK-040",
        title="Deprecated: Biased locking",
        description="Biased locking disabled by default in JDK 15, deprecated in JDK 17.",
        severity=Severity.LOW,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'-XX:\+UseBiasedLocking'),
        suggestion="Remove flag; biased locking provides minimal benefit on modern hardware",
        documentation_url="https://openjdk.org/jeps/374"
    ),

    # API Changes
    ScanRule(
        rule_id="JDK-050",
        title="Changed: URL constructor",
        description="URL constructor deprecated in favor of URI.toURL().",
        severity=Severity.LOW,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'new\s+URL\s*\(\s*["\']'),
        suggestion="Use URI.create(string).toURL() instead",
    ),
    ScanRule(
        rule_id="JDK-051",
        title="Changed: finalize() method",
        description="Object.finalize() deprecated for removal.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'protected\s+void\s+finalize\s*\(|@Override.*finalize'),
        suggestion="Use try-with-resources, Cleaner, or PhantomReference",
        documentation_url="https://openjdk.org/jeps/421"
    ),
    ScanRule(
        rule_id="JDK-052",
        title="Deprecated: Thread.stop()",
        description="Thread.stop() is deprecated and may be removed.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'\.stop\s*\(\s*\)|Thread\.stop'),
        suggestion="Use interrupt() and cooperative thread termination",
    ),
    ScanRule(
        rule_id="JDK-053",
        title="Removed: Thread.destroy() and Thread.suspend()",
        description="Thread.destroy() and suspend() removed in JDK 11.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'\.(?:destroy|suspend|resume)\s*\(\s*\)'),
        suggestion="Use proper thread coordination mechanisms",
    ),
    ScanRule(
        rule_id="JDK-054",
        title="Changed: Locale data",
        description="CLDR locale data is default in JDK 9+; may affect formatting.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'DateFormat\.getInstance|NumberFormat\.getInstance|SimpleDateFormat'),
        suggestion="Test locale-sensitive formatting; use -Djava.locale.providers=COMPAT if needed",
    ),
    ScanRule(
        rule_id="JDK-055",
        title="Changed: Default charset",
        description="Default charset is UTF-8 in JDK 18+; affects file I/O.",
        severity=Severity.MEDIUM,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'new\s+(?:FileReader|FileWriter|InputStreamReader|OutputStreamWriter)\s*\([^)]*\)(?!.*Charset|charset|UTF)'),
        suggestion="Explicitly specify charset: new FileReader(file, StandardCharsets.UTF_8)",
    ),
    ScanRule(
        rule_id="JDK-056",
        title="Deprecated: Primitive wrapper constructors",
        description="Constructors like new Integer(x) deprecated; use valueOf().",
        severity=Severity.LOW,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'new\s+(?:Integer|Long|Double|Float|Short|Byte|Boolean|Character)\s*\('),
        suggestion="Use Integer.valueOf(), autoboxing, or static factory methods",
    ),
    ScanRule(
        rule_id="JDK-057",
        title="Changed: Sealed classes (preview feature)",
        description="Check for sealed class usage if using preview features.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'\bsealed\s+(?:class|interface)|permits\s+'),
        suggestion="Sealed classes finalized in JDK 17; ensure preview flags removed",
    ),
    ScanRule(
        rule_id="JDK-058",
        title="Changed: Record classes",
        description="Record classes finalized in JDK 16.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'\brecord\s+\w+\s*\('),
        suggestion="Records are production-ready in JDK 16+; remove --enable-preview if used",
    ),
    ScanRule(
        rule_id="JDK-059",
        title="Changed: Pattern matching for instanceof",
        description="Pattern matching instanceof finalized in JDK 16.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'instanceof\s+\w+\s+\w+(?:\s*&&|\s*\{|\s*\))'),
        suggestion="Pattern matching is production-ready in JDK 16+",
    ),
    ScanRule(
        rule_id="JDK-060",
        title="Changed: Text blocks",
        description="Text blocks finalized in JDK 15.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'"""'),
        suggestion="Text blocks are production-ready in JDK 15+",
    ),

    # Module System Issues
    ScanRule(
        rule_id="JDK-070",
        title="Module: Split package detected",
        description="Split packages (same package in multiple JARs) cause issues with modules.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'--patch-module'),
        suggestion="Resolve split packages by relocating or merging",
    ),
    ScanRule(
        rule_id="JDK-071",
        title="Module: Illegal access workaround",
        description="--illegal-access flag removed in JDK 17.",
        severity=Severity.HIGH,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'--illegal-access=(?:permit|warn|debug|deny)'),
        suggestion="Replace with specific --add-opens/--add-exports flags",
        documentation_url="https://openjdk.org/jeps/403"
    ),
    ScanRule(
        rule_id="JDK-072",
        title="Module: Add-opens for common frameworks",
        description="Common frameworks may need --add-opens flags.",
        severity=Severity.INFO,
        category=Category.JDK_MIGRATION,
        pattern=re.compile(r'--add-opens'),
        suggestion="Document required --add-opens flags for deployment",
    ),
]

# =============================================================================
# PYTHON 2 -> 3 MIGRATION RULES
# =============================================================================

PYTHON_MIGRATION_RULES = [
    # Print Statement
    ScanRule(
        rule_id="PY-001",
        title="Syntax: print statement",
        description="print is a function in Python 3, not a statement.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'^\s*print\s+[^(]|^\s*print\s*$', re.MULTILINE),
        suggestion="Use print() function: print('message')",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-002",
        title="Syntax: print with >>",
        description="print >> syntax removed in Python 3.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'print\s*>>'),
        suggestion="Use print(message, file=sys.stderr)",
        file_extensions=(".py",)
    ),

    # Division
    ScanRule(
        rule_id="PY-010",
        title="Changed: Integer division",
        description="/ returns float in Python 3; use // for integer division.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?<![/\w])/(?![/=])'),
        suggestion="Use // for integer division; verify / behavior is intended",
        file_extensions=(".py",)
    ),

    # Unicode/Strings
    ScanRule(
        rule_id="PY-020",
        title="Removed: unicode() builtin",
        description="unicode() removed; str is unicode in Python 3.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bunicode\s*\('),
        suggestion="Use str() instead of unicode()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-021",
        title="Changed: basestring type",
        description="basestring removed; use str instead.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bbasestring\b'),
        suggestion="Use str instead of basestring",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-022",
        title="Changed: String prefix u''",
        description="u'' prefix is optional in Python 3 (all strings are unicode).",
        severity=Severity.LOW,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r"u['\"]"),
        suggestion="u'' prefix is optional in Python 3; can be removed",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-023",
        title="Changed: String encode/decode",
        description="str.encode() returns bytes; bytes.decode() returns str.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.encode\s*\(\s*\)|\.decode\s*\(\s*\)'),
        suggestion="Explicitly specify encoding: .encode('utf-8')",
        file_extensions=(".py",)
    ),

    # Iterators and Ranges
    ScanRule(
        rule_id="PY-030",
        title="Removed: xrange()",
        description="xrange() removed; use range() in Python 3.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bxrange\s*\('),
        suggestion="Use range() instead of xrange()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-031",
        title="Changed: dict.keys()/values()/items()",
        description="dict methods return views, not lists in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.(?:keys|values|items)\s*\(\s*\)\s*\['),
        suggestion="Wrap in list() if indexing needed: list(d.keys())[0]",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-032",
        title="Removed: dict.iterkeys()/itervalues()/iteritems()",
        description="dict.iter* methods removed; use keys()/values()/items().",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.(?:iterkeys|itervalues|iteritems)\s*\('),
        suggestion="Use .keys()/.values()/.items() instead",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-033",
        title="Removed: dict.has_key()",
        description="dict.has_key() removed; use 'in' operator.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.has_key\s*\('),
        suggestion="Use 'key in dict' instead of dict.has_key(key)",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-034",
        title="Changed: map/filter/zip return iterators",
        description="map(), filter(), zip() return iterators in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?:map|filter|zip)\s*\([^)]+\)\s*\['),
        suggestion="Wrap in list() if indexing needed: list(map(...))[0]",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-035",
        title="Removed: reduce() builtin",
        description="reduce() moved to functools module.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?<!functools\.)\breduce\s*\('),
        suggestion="Use functools.reduce() or rewrite with explicit loop",
        file_extensions=(".py",)
    ),

    # Imports
    ScanRule(
        rule_id="PY-040",
        title="Changed: Relative imports",
        description="Implicit relative imports removed; use explicit relative imports.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'^from\s+(?!\.|__future__|sys|os|re|json|typing|collections|datetime)(?!.*import\s+\()\w+\s+import', re.MULTILINE),
        suggestion="Use explicit relative imports: from . import module",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-041",
        title="Renamed: ConfigParser module",
        description="ConfigParser renamed to configparser (lowercase).",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+ConfigParser|from\s+ConfigParser'),
        suggestion="Use: import configparser",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-042",
        title="Renamed: Queue module",
        description="Queue module renamed to queue (lowercase).",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+Queue(?!\w)|from\s+Queue\s+import'),
        suggestion="Use: import queue",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-043",
        title="Renamed: SocketServer module",
        description="SocketServer renamed to socketserver.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+SocketServer|from\s+SocketServer'),
        suggestion="Use: import socketserver",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-044",
        title="Renamed: urllib/urllib2",
        description="urllib and urllib2 reorganized in Python 3.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+urllib2|from\s+urllib2|import\s+urlparse|from\s+urlparse'),
        suggestion="Use urllib.request, urllib.parse, urllib.error",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-045",
        title="Renamed: httplib module",
        description="httplib renamed to http.client.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+httplib|from\s+httplib'),
        suggestion="Use: from http import client as httplib",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-046",
        title="Renamed: cPickle module",
        description="cPickle merged into pickle.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+cPickle|from\s+cPickle'),
        suggestion="Use: import pickle (automatically optimized in Python 3)",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-047",
        title="Renamed: StringIO module",
        description="StringIO and cStringIO moved to io module.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?:import|from)\s+(?:c?StringIO)'),
        suggestion="Use: from io import StringIO, BytesIO",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-048",
        title="Renamed: thread module",
        description="thread module renamed to _thread.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+thread(?!\w|ing)|from\s+thread\s+import'),
        suggestion="Use: import _thread or preferably threading module",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-049",
        title="Renamed: Tkinter module",
        description="Tkinter renamed to tkinter.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'import\s+Tkinter|from\s+Tkinter'),
        suggestion="Use: import tkinter",
        file_extensions=(".py",)
    ),

    # Exception Handling
    ScanRule(
        rule_id="PY-050",
        title="Syntax: except Exception, e",
        description="Old except syntax removed; use 'as' keyword.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'except\s+\w+\s*,\s*\w+\s*:'),
        suggestion="Use: except Exception as e:",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-051",
        title="Changed: raise syntax",
        description="raise Exception, 'message' syntax removed.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'raise\s+\w+\s*,\s*["\']'),
        suggestion="Use: raise Exception('message')",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-052",
        title="Changed: StandardError base class",
        description="StandardError removed; use Exception instead.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bStandardError\b'),
        suggestion="Use Exception as base class",
        file_extensions=(".py",)
    ),

    # Comparisons
    ScanRule(
        rule_id="PY-060",
        title="Removed: cmp() builtin",
        description="cmp() removed; use (a > b) - (a < b) pattern.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bcmp\s*\('),
        suggestion="Use comparison operators or functools.cmp_to_key()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-061",
        title="Removed: __cmp__ method",
        description="__cmp__ removed; use __lt__, __eq__, etc.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'def\s+__cmp__\s*\('),
        suggestion="Implement __lt__, __eq__, __le__, etc. or use @functools.total_ordering",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-062",
        title="Changed: Comparison operators",
        description="Comparing different types raises TypeError in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?:None|True|False)\s*[<>]\s*\d|sort\s*\(\s*\)'),
        suggestion="Ensure type consistency in comparisons",
        file_extensions=(".py",)
    ),

    # Classes
    ScanRule(
        rule_id="PY-070",
        title="Changed: Old-style classes",
        description="Old-style classes removed; all classes inherit from object.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'class\s+\w+\s*:\s*(?:$|\n)', re.MULTILINE),
        suggestion="Explicit object inheritance optional in Python 3 but recommended for clarity",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-071",
        title="Changed: metaclass syntax",
        description="__metaclass__ attribute doesn't work in Python 3.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'__metaclass__\s*='),
        suggestion="Use: class Foo(metaclass=MyMeta):",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-072",
        title="Removed: unbound methods",
        description="Unbound methods are plain functions in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.im_func|\.im_self'),
        suggestion="Use __func__ and __self__ attributes",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-073",
        title="Changed: super() syntax",
        description="super() can be called without arguments in Python 3.",
        severity=Severity.INFO,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'super\s*\(\s*\w+\s*,\s*self\s*\)'),
        suggestion="Can simplify to super() in Python 3",
        file_extensions=(".py",)
    ),

    # Builtins
    ScanRule(
        rule_id="PY-080",
        title="Removed: raw_input()",
        description="raw_input() renamed to input() in Python 3.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\braw_input\s*\('),
        suggestion="Use input() instead of raw_input()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-081",
        title="Changed: input() behavior",
        description="input() no longer eval()s in Python 3.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\binput\s*\([^)]*\)'),
        suggestion="Python 3 input() returns string; old input() was eval(raw_input())",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-082",
        title="Removed: execfile()",
        description="execfile() removed; use exec(open(f).read()).",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bexecfile\s*\('),
        suggestion="Use: exec(open(filename).read())",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-083",
        title="Removed: reload() builtin",
        description="reload() moved to importlib.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?<!importlib\.)\breload\s*\('),
        suggestion="Use: importlib.reload(module)",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-084",
        title="Removed: apply() builtin",
        description="apply() removed; use func(*args, **kwargs).",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bapply\s*\('),
        suggestion="Use: func(*args, **kwargs)",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-085",
        title="Removed: long type",
        description="long type removed; int handles arbitrary precision.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\blong\s*\(|\bL\s*$|\d+L\b'),
        suggestion="Use int instead of long",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-086",
        title="Removed: buffer() builtin",
        description="buffer() removed; use memoryview().",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bbuffer\s*\('),
        suggestion="Use memoryview() instead of buffer()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-087",
        title="Removed: coerce() builtin",
        description="coerce() removed in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bcoerce\s*\('),
        suggestion="Remove coerce() usage; handle type conversion explicitly",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-088",
        title="Removed: intern() builtin",
        description="intern() moved to sys module.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?<!sys\.)\bintern\s*\('),
        suggestion="Use: sys.intern()",
        file_extensions=(".py",)
    ),

    # File Handling
    ScanRule(
        rule_id="PY-090",
        title="Removed: file() builtin",
        description="file() removed; use open().",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\bfile\s*\([^)]+\)'),
        suggestion="Use open() instead of file()",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-091",
        title="Changed: File modes",
        description="Binary mode required for bytes in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r"open\s*\([^)]+['\"][rwa]['\"]"),
        suggestion="Use 'rb', 'wb' for binary; 'r', 'w' for text with encoding",
        file_extensions=(".py",)
    ),

    # Operators and Syntax
    ScanRule(
        rule_id="PY-100",
        title="Removed: Backtick repr",
        description="Backticks for repr removed; use repr().",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'`[^`]+`'),
        suggestion="Use repr() instead of backticks",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-101",
        title="Removed: <> comparison",
        description="<> operator removed; use != instead.",
        severity=Severity.CRITICAL,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'<>'),
        suggestion="Use != instead of <>",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-102",
        title="Changed: Octal literals",
        description="Octal literals must use 0o prefix.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'(?<![0-9xXbBoO])0[0-7]+(?![0-9])'),
        suggestion="Use 0o prefix for octal: 0o755 instead of 0755",
        file_extensions=(".py",)
    ),

    # PySpark-Specific Python 3
    ScanRule(
        rule_id="PY-110",
        title="PySpark: Python 2 lambda syntax",
        description="Python 2 lambda tuple unpacking doesn't work in Python 3.",
        severity=Severity.HIGH,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'lambda\s*\(\s*\w+\s*,\s*\w+\s*\)\s*:'),
        suggestion="Use: lambda x: (x[0], x[1]) instead of lambda (a, b):",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-111",
        title="PySpark: Bytes vs String in RDD",
        description="RDD operations may need encoding fixes for Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.textFile\s*\(|\.saveAsTextFile\s*\('),
        suggestion="Ensure string encoding consistency in RDD operations",
        file_extensions=(".py",)
    ),
    ScanRule(
        rule_id="PY-112",
        title="PySpark: Python 2 sorting with None",
        description="Sorting with None values fails in Python 3.",
        severity=Severity.MEDIUM,
        category=Category.PYTHON_MIGRATION,
        pattern=re.compile(r'\.sortBy\s*\(|\.sortByKey\s*\('),
        suggestion="Handle None values explicitly in sort key functions",
        file_extensions=(".py",)
    ),
]

# Combine all rules
ALL_RULES = (
    SPARK_API_RULES +
    SPARK_SQL_RULES +
    SPARK_CONFIG_RULES +
    SPARK_DEPENDENCY_RULES +
    HDFS_TO_S3_RULES +
    PERFORMANCE_RULES +
    JDK_MIGRATION_RULES +
    PYTHON_MIGRATION_RULES
)


class SparkMigrationScanner:
    """Scanner for Spark 2.4 -> 3.5 and HDFS -> S3 migration issues."""

    def __init__(self, rules: list[ScanRule] = None):
        self.rules = rules or ALL_RULES
        self.issues: list[Issue] = []
        self.files_scanned = 0
        self.lines_scanned = 0

    def scan_file(self, file_path: Path) -> list[Issue]:
        """Scan a single file for migration issues."""
        issues = []

        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
        except Exception as e:
            logger.warning(f"Could not read {file_path}: {e}")
            return issues

        self.lines_scanned += len(lines)

        for rule in self.rules:
            # Check file extension
            if not str(file_path).endswith(rule.file_extensions):
                continue

            for line_num, line in enumerate(lines, 1):
                if rule.pattern.search(line):
                    issue = Issue(
                        rule_id=rule.rule_id,
                        title=rule.title,
                        description=rule.description,
                        severity=rule.severity,
                        category=rule.category,
                        file_path=str(file_path),
                        line_number=line_num,
                        line_content=line.strip()[:200],  # Truncate long lines
                        suggestion=rule.suggestion,
                        documentation_url=rule.documentation_url
                    )
                    issues.append(issue)

        return issues

    def scan_directory(self, directory: Path, exclude_patterns: list[str] = None) -> list[Issue]:
        """Recursively scan a directory for migration issues."""
        exclude_patterns = exclude_patterns or [
            '__pycache__', '.git', 'node_modules', '.venv', 'venv',
            'target', 'build', 'dist', '.idea', '.vscode', '*.pyc'
        ]

        all_issues = []

        logger.debug(f"Scanning directory: {directory}")
        logger.debug(f"Exclude patterns: {exclude_patterns}")

        for root, dirs, files in os.walk(directory):
            # Filter excluded directories
            dirs[:] = [d for d in dirs if not any(
                d == pat or (pat.startswith('*') and d.endswith(pat[1:]))
                for pat in exclude_patterns
            )]

            for file in files:
                file_path = Path(root) / file

                # Skip excluded files
                if any(file == pat or (pat.startswith('*') and file.endswith(pat[1:]))
                       for pat in exclude_patterns):
                    continue

                # Check if file has scannable extension
                if any(str(file_path).endswith(ext)
                       for ext in ('.py', '.scala', '.java', '.sql', '.conf',
                                   '.properties', '.xml', '.yaml', '.yml', '.sbt', '.gradle', '.hql')):
                    self.files_scanned += 1
                    logger.debug(f"Scanning file: {file_path}")
                    issues = self.scan_file(file_path)
                    all_issues.extend(issues)
                    if issues:
                        logger.debug(f"  Found {len(issues)} issues in {file_path}")

        self.issues = all_issues
        logger.debug(f"Directory scan complete. Total issues: {len(all_issues)}")
        return all_issues

    def scan_path(self, path: str) -> list[Issue]:
        """Scan a file or directory."""
        p = Path(path)
        if p.is_file():
            self.files_scanned = 1
            self.issues = self.scan_file(p)
            return self.issues
        elif p.is_dir():
            return self.scan_directory(p)
        else:
            raise ValueError(f"Path does not exist: {path}")

    def get_summary(self) -> dict:
        """Get summary statistics of the scan."""
        severity_counts = defaultdict(int)
        category_counts = defaultdict(int)

        for issue in self.issues:
            severity_counts[issue.severity.value] += 1
            category_counts[issue.category.value] += 1

        return {
            "files_scanned": self.files_scanned,
            "lines_scanned": self.lines_scanned,
            "total_issues": len(self.issues),
            "by_severity": dict(severity_counts),
            "by_category": dict(category_counts)
        }

    def to_markdown(self) -> str:
        """Generate markdown report."""
        summary = self.get_summary()

        lines = [
            "# Spark Migration Scan Report",
            "",
            f"**Scan Date:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Summary",
            "",
            f"- **Files Scanned:** {summary['files_scanned']:,}",
            f"- **Lines Scanned:** {summary['lines_scanned']:,}",
            f"- **Total Issues Found:** {summary['total_issues']:,}",
            "",
            "### Issues by Severity",
            "",
        ]

        severity_order = ['critical', 'high', 'medium', 'low', 'info']
        severity_emoji = {
            'critical': '🔴',
            'high': '🟠',
            'medium': '🟡',
            'low': '🔵',
            'info': 'ℹ️'
        }

        for sev in severity_order:
            count = summary['by_severity'].get(sev, 0)
            if count > 0:
                lines.append(f"- {severity_emoji.get(sev, '')} **{sev.upper()}:** {count}")

        lines.extend([
            "",
            "### Issues by Category",
            "",
        ])

        for cat, count in sorted(summary['by_category'].items(), key=lambda x: -x[1]):
            lines.append(f"- **{cat}:** {count}")

        # Group issues by category and severity
        issues_by_category = defaultdict(list)
        for issue in self.issues:
            issues_by_category[issue.category.value].append(issue)

        lines.extend([
            "",
            "---",
            "",
            "## Detailed Findings",
            "",
        ])

        for category in Category:
            cat_issues = issues_by_category.get(category.value, [])
            if not cat_issues:
                continue

            lines.extend([
                f"### {category.value}",
                "",
            ])

            # Sort by severity
            cat_issues.sort(key=lambda x: severity_order.index(x.severity.value))

            # Group by rule
            issues_by_rule = defaultdict(list)
            for issue in cat_issues:
                issues_by_rule[issue.rule_id].append(issue)

            for rule_id, rule_issues in issues_by_rule.items():
                first = rule_issues[0]
                lines.extend([
                    f"#### {severity_emoji.get(first.severity.value, '')} [{rule_id}] {first.title}",
                    "",
                    f"**Severity:** {first.severity.value.upper()}  ",
                    f"**Description:** {first.description}  ",
                    f"**Suggestion:** {first.suggestion}",
                    "",
                ])

                if first.documentation_url:
                    lines.append(f"**Documentation:** [{first.documentation_url}]({first.documentation_url})")
                    lines.append("")

                lines.append("**Occurrences:**")
                lines.append("")

                for issue in rule_issues[:20]:  # Limit to first 20 occurrences per rule
                    lines.append(f"- `{issue.file_path}:{issue.line_number}`")
                    lines.append("  ```")
                    lines.append(f"  {issue.line_content}")
                    lines.append("  ```")

                if len(rule_issues) > 20:
                    lines.append(f"- ... and {len(rule_issues) - 20} more occurrences")

                lines.append("")

        # Add migration checklist
        lines.extend([
            "---",
            "",
            "## Migration Checklist",
            "",
            "### Spark 2.4 → 3.5 Migration",
            "",
            "- [ ] Update Scala version to 2.12 or 2.13",
            "- [ ] Update Java version if needed (8, 11, or 17 supported)",
            "- [ ] Replace SQLContext/HiveContext with SparkSession",
            "- [ ] Update deprecated DataFrame APIs (registerTempTable → createOrReplaceTempView)",
            "- [ ] Review and update UDF type annotations",
            "- [ ] Update Pandas UDF syntax if using @pandas_udf",
            "- [ ] Migrate from spark.mllib to spark.ml if applicable",
            "- [ ] Review SQL queries for ANSI mode compatibility",
            "- [ ] Update datetime parsing patterns",
            "- [ ] Enable Adaptive Query Execution (AQE)",
            "- [ ] Update all dependency versions",
            "- [ ] Retrain ML models if using persisted models",
            "",
            "### HDFS → S3 Migration",
            "",
            "- [ ] Convert all hdfs:// paths to s3a://",
            "- [ ] Configure S3A credentials (IAM roles preferred)",
            "- [ ] Configure S3A committers for reliable writes",
            "- [ ] Remove HDFS-specific configs (replication, block size)",
            "- [ ] Review and optimize for S3 access patterns",
            "- [ ] Consider Delta Lake/Iceberg for transactional workloads",
            "- [ ] Remove any S3Guard/consistency workarounds (no longer needed)",
            "- [ ] Enable S3 server-side encryption",
            "- [ ] Test write/rename/delete operations",
            "- [ ] Benchmark performance and adjust partition sizes",
            "",
            "### JDK 8/11 → 17 Migration",
            "",
            "- [ ] Remove/replace removed Java EE modules (JAXB, JAX-WS, CORBA, etc.)",
            "- [ ] Add Jakarta EE dependencies for removed javax.* packages",
            "- [ ] Update sun.misc.* and com.sun.* internal API usage",
            "- [ ] Replace deprecated GC flags (CMS → G1/ZGC)",
            "- [ ] Update JVM logging flags to Unified Logging (-Xlog:gc*)",
            "- [ ] Remove obsolete flags (PermSize, AggressiveOpts, etc.)",
            "- [ ] Add required --add-opens/--add-exports for reflection",
            "- [ ] Remove --illegal-access flag (not supported in 17)",
            "- [ ] Update finalize() methods to use Cleaner/try-with-resources",
            "- [ ] Review and test locale-sensitive formatting",
            "- [ ] Verify library compatibility with JDK 17",
            "",
            "### Python 2 → 3 Migration",
            "",
            "- [ ] Convert print statements to print() function",
            "- [ ] Update exception syntax (except E, e: → except E as e:)",
            "- [ ] Replace unicode()/basestring with str",
            "- [ ] Replace xrange() with range()",
            "- [ ] Update dict methods (.iteritems() → .items())",
            "- [ ] Fix renamed modules (ConfigParser → configparser, etc.)",
            "- [ ] Update urllib/urllib2 to urllib.request/parse/error",
            "- [ ] Replace raw_input() with input()",
            "- [ ] Update integer division (/ vs //)",
            "- [ ] Fix octal literals (0755 → 0o755)",
            "- [ ] Update string encoding/decoding for bytes vs str",
            "- [ ] Review lambda functions for tuple unpacking issues",
            "- [ ] Test sorting with None values",
            "",
        ])

        return '\n'.join(lines)

    def to_json(self) -> str:
        """Generate JSON report."""
        summary = self.get_summary()

        issues_data = []
        for issue in self.issues:
            issue_dict = asdict(issue)
            issue_dict['severity'] = issue.severity.value
            issue_dict['category'] = issue.category.value
            issues_data.append(issue_dict)

        report = {
            "summary": summary,
            "issues": issues_data
        }

        return json.dumps(report, indent=2)

    def to_html(self) -> str:
        """Generate HTML report."""
        summary = self.get_summary()

        severity_colors = {
            'critical': '#dc3545',
            'high': '#fd7e14',
            'medium': '#ffc107',
            'low': '#17a2b8',
            'info': '#6c757d'
        }

        html = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Spark Migration Scan Report</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #007bff; padding-bottom: 10px; }}
        h2 {{ color: #444; margin-top: 30px; }}
        h3 {{ color: #555; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .stat-card {{ background: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }}
        .stat-card .number {{ font-size: 2em; font-weight: bold; color: #007bff; }}
        .stat-card .label {{ color: #666; }}
        .severity-badge {{ padding: 4px 8px; border-radius: 4px; color: white; font-size: 0.8em; font-weight: bold; }}
        .issue {{ background: #f8f9fa; margin: 15px 0; padding: 15px; border-radius: 8px; border-left: 4px solid; }}
        .issue code {{ background: #e9ecef; padding: 2px 6px; border-radius: 3px; font-size: 0.9em; }}
        .issue pre {{ background: #282c34; color: #abb2bf; padding: 10px; border-radius: 4px; overflow-x: auto; }}
        .suggestion {{ background: #d4edda; padding: 10px; border-radius: 4px; margin-top: 10px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background: #f8f9fa; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>🔍 Spark Migration Scan Report</h1>
        <p><strong>Generated:</strong> {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

        <div class="summary">
            <div class="stat-card">
                <div class="number">{summary['files_scanned']:,}</div>
                <div class="label">Files Scanned</div>
            </div>
            <div class="stat-card">
                <div class="number">{summary['lines_scanned']:,}</div>
                <div class="label">Lines Scanned</div>
            </div>
            <div class="stat-card">
                <div class="number">{summary['total_issues']:,}</div>
                <div class="label">Issues Found</div>
            </div>
        </div>

        <h2>Issues by Severity</h2>
        <table>
            <tr><th>Severity</th><th>Count</th></tr>
'''

        for sev in ['critical', 'high', 'medium', 'low', 'info']:
            count = summary['by_severity'].get(sev, 0)
            color = severity_colors.get(sev, '#666')
            html += f'            <tr><td><span class="severity-badge" style="background:{color}">{sev.upper()}</span></td><td>{count}</td></tr>\n'

        html += '''        </table>

        <h2>Issues by Category</h2>
        <table>
            <tr><th>Category</th><th>Count</th></tr>
'''

        for cat, count in sorted(summary['by_category'].items(), key=lambda x: -x[1]):
            html += f'            <tr><td>{cat}</td><td>{count}</td></tr>\n'

        html += '''        </table>

        <h2>Detailed Findings</h2>
'''

        # Group by category
        issues_by_category = defaultdict(list)
        for issue in self.issues:
            issues_by_category[issue.category.value].append(issue)

        for category in Category:
            cat_issues = issues_by_category.get(category.value, [])
            if not cat_issues:
                continue

            html += f'        <h3>{category.value} ({len(cat_issues)} issues)</h3>\n'

            # Group by rule
            issues_by_rule = defaultdict(list)
            for issue in cat_issues:
                issues_by_rule[issue.rule_id].append(issue)

            for rule_id, rule_issues in issues_by_rule.items():
                first = rule_issues[0]
                color = severity_colors.get(first.severity.value, '#666')

                html += f'''        <div class="issue" style="border-color:{color}">
            <strong><span class="severity-badge" style="background:{color}">{first.severity.value.upper()}</span> [{rule_id}] {first.title}</strong>
            <p>{first.description}</p>
            <div class="suggestion">💡 <strong>Suggestion:</strong> {first.suggestion}</div>
            <p><strong>Found in {len(rule_issues)} location(s):</strong></p>
            <ul>
'''
                for issue in rule_issues[:10]:
                    html += f'                <li><code>{issue.file_path}:{issue.line_number}</code><pre>{issue.line_content}</pre></li>\n'

                if len(rule_issues) > 10:
                    html += f'                <li><em>... and {len(rule_issues) - 10} more</em></li>\n'

                html += '''            </ul>
        </div>
'''

        html += '''    </div>
</body>
</html>'''

        return html


def main():
    parser = argparse.ArgumentParser(
        description='Scan code for Spark 2.4 -> 3.5 and HDFS -> S3 migration issues'
    )
    parser.add_argument('path', help='File or directory to scan')
    parser.add_argument('--output', '-o', help='Output file path')
    parser.add_argument('--format', '-f', choices=['md', 'json', 'html'], default='md',
                        help='Output format (default: md)')
    parser.add_argument('--severity', '-s', choices=['critical', 'high', 'medium', 'low', 'info'],
                        help='Minimum severity to report')
    parser.add_argument('--category', '-c', action='append',
                        choices=[c.value for c in Category],
                        help='Categories to include (can be specified multiple times)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose/debug logging')
    parser.add_argument('--quiet', '-q', action='store_true',
                        help='Suppress info messages, only show warnings and errors')

    args = parser.parse_args()

    # Configure logging level based on arguments
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    # Filter rules if specified
    rules = ALL_RULES

    if args.severity:
        severity_order = ['critical', 'high', 'medium', 'low', 'info']
        min_idx = severity_order.index(args.severity)
        rules = [r for r in rules if severity_order.index(r.severity.value) <= min_idx]
        logger.debug(f"Filtered to {len(rules)} rules with severity >= {args.severity}")

    if args.category:
        rules = [r for r in rules if r.category.value in args.category]
        logger.debug(f"Filtered to {len(rules)} rules in categories: {args.category}")

    # Run scanner
    scanner = SparkMigrationScanner(rules)

    logger.info(f"Scanning {args.path}...")

    try:
        scanner.scan_path(args.path)
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    summary = scanner.get_summary()
    logger.info("Scan complete!")
    logger.info(f"  Files scanned: {summary['files_scanned']:,}")
    logger.info(f"  Lines scanned: {summary['lines_scanned']:,}")
    logger.info(f"  Issues found:  {summary['total_issues']:,}")

    # Log severity breakdown at debug level
    if summary['by_severity']:
        logger.debug("Issues by severity:")
        for sev, count in sorted(summary['by_severity'].items()):
            logger.debug(f"    {sev}: {count}")

    # Generate report
    if args.format == 'md':
        report = scanner.to_markdown()
    elif args.format == 'json':
        report = scanner.to_json()
    else:
        report = scanner.to_html()

    # Output
    if args.output:
        try:
            with open(args.output, 'w') as f:
                f.write(report)
            logger.info(f"Report written to: {args.output}")
        except IOError as e:
            logger.error(f"Failed to write report to {args.output}: {e}")
            sys.exit(1)
    else:
        print("\n" + "=" * 60)
        print(report)


if __name__ == '__main__':
    main()
