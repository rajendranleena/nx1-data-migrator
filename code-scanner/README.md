# Spark Migration Code Scanner

A comprehensive static analysis tool for identifying issues when migrating:

- **Apache Spark** 2.4 → 3.5
- **Storage** HDFS → S3
- **JDK** 8/11 → 17
- **Python** 2 → 3

## Overview

This scanner analyzes your codebase for:

- **Spark API Changes**: Removed/deprecated APIs, behavioral changes, and syntax updates
- **Spark SQL Changes**: Query compatibility, function changes, and type handling
- **Configuration Changes**: Renamed/removed configs, new optimization options
- **Dependency Changes**: Scala, Java, Python, and library version requirements
- **HDFS to S3 Migration**: Path conversions, performance patterns, and S3-specific configurations
- **JDK 8/11 → 17 Migration**: Removed APIs, encapsulated internals, JVM flag changes
- **Python 2 → 3 Migration**: Syntax changes, renamed modules, removed builtins
- **Performance Considerations**: Anti-patterns and optimization opportunities
- **Security Issues**: Credential handling and encryption settings

## Quick Start

```bash
# Scan a directory
python code-scanner.py /path/to/your/project

# Generate HTML report
python code-scanner.py /path/to/project -f html -o report.html

# Generate JSON report (for CI/CD integration)
python code-scanner.py /path/to/project -f json -o report.json

# Filter by severity
python code-scanner.py /path/to/project -s high

# Filter by category
python code-scanner.py /path/to/project -c "Spark API Changes" -c "HDFS to S3 Migration"
```

## Supported File Types

The scanner analyzes:

| Extension       | Language/Format             |
| --------------- | --------------------------- |
| `.py`           | Python (PySpark)            |
| `.scala`        | Scala                       |
| `.java`         | Java                        |
| `.sql`          | SQL files                   |
| `.conf`         | Configuration files         |
| `.properties`   | Java properties             |
| `.xml`          | XML configs (pom.xml, etc.) |
| `.yaml`, `.yml` | YAML configs                |
| `.sbt`          | SBT build files             |
| `.gradle`       | Gradle build files          |

## Severity Levels

| Level        | Emoji | Description                                      |
| ------------ | ----- | ------------------------------------------------ |
| **CRITICAL** | 🔴    | Will break at runtime; must fix before migration |
| **HIGH**     | 🟠    | Likely to cause failures or data issues          |
| **MEDIUM**   | 🟡    | Behavioral changes that may affect results       |
| **LOW**      | 🔵    | Deprecation warnings, style issues               |
| **INFO**     | ℹ️    | Optimization opportunities, recommendations      |

## How the Scanner Works

### Single-line scanning

For most rules, the scanner reads each file line-by-line and applies the rule's compiled regex. If a match is found, an `Issue` is recorded with the file path and 1-based line number.

### Multi-line scanning

Rules with `multiline=True` use a second pass over the full file content with `re.DOTALL`, enabling patterns that span method-call arguments across multiple lines — for example:

```python
# Detected even though the argument is on the next line
df.registerTempTable(
    "my_view"
)
```

Multi-line scanning is skipped for files larger than 1 MB to protect performance. A warning is logged when this limit is hit.

### AST-aware filtering (Python files only)

Rules with `ast_aware=True` run through an `ASTCodeExtractor` that parses the file with Python's `ast` module and identifies which line numbers contain real executable code. Matches on comment lines and docstrings are silently discarded, eliminating a common source of false positives:

```python
# sqlContext was the old way — this line is NOT reported
spark = SparkSession.builder.getOrCreate()  # this line IS reported if it matched
```

If the file contains a syntax error, AST parsing is skipped and all matched lines are treated as executable (no issues are missed).

### Deduplication

The scanner tracks `(rule_id, line_number)` pairs so a single line can never appear twice for the same rule, even when both the single-line and multi-line passes match it.

## Issue Categories

### Spark API Changes (SPARK-xxx)

Covers removed and deprecated APIs:

| Rule ID   | Issue                       | Action Required                              |
| --------- | --------------------------- | -------------------------------------------- |
| SPARK-001 | SQLContext removed          | Use SparkSession                             |
| SPARK-002 | HiveContext removed         | Use SparkSession.builder.enableHiveSupport() |
| SPARK-003 | registerTempTable removed   | Use createOrReplaceTempView                  |
| SPARK-009 | unionAll renamed            | Use union                                    |
| SPARK-010 | DataFrame.explode removed   | Use functions.explode with select            |
| SPARK-014 | UDF type inference stricter | Explicitly specify return types              |
| SPARK-015 | @pandas_udf syntax changed  | Update to new decorator syntax               |
| SPARK-030 | spark.mllib deprecated      | Migrate to spark.ml                          |

### Spark SQL Changes (SQL-xxx)

Query compatibility and function changes:

| Rule ID | Issue                             | Impact                                          |
| ------- | --------------------------------- | ----------------------------------------------- |
| SQL-001 | CAST behavior changed             | Invalid values return NULL instead of exception |
| SQL-003 | from_json requires schema         | No more schema inference                        |
| SQL-004 | Date/timestamp parsing stricter   | Must use explicit formats                       |
| SQL-012 | CREATE TABLE defaults changed     | Explicitly specify USING clause                 |
| SQL-013 | INSERT OVERWRITE behavior changed | Check partitionOverwriteMode setting            |
| SQL-015 | Datetime format patterns changed  | Use Java 8 patterns (yyyy not YYYY)             |

### Configuration Changes (CFG-xxx)

Settings that need attention:

| Rule ID | Config                             | Change                                        |
| ------- | ---------------------------------- | --------------------------------------------- |
| CFG-003 | spark.sql.adaptive.enabled         | Now available (enable for better performance) |
| CFG-004 | spark.sql.execution.arrow.enabled  | Renamed to .arrow.pyspark.enabled             |
| CFG-005 | spark.sql.legacy.timeParserPolicy  | Review for datetime compatibility             |
| CFG-006 | spark.sql.parquet.int96AsTimestamp | Use outputTimestampType instead               |

### Dependency Changes (DEP-xxx)

Version compatibility requirements:

| Rule ID | Dependency | Requirement                       |
| ------- | ---------- | --------------------------------- |
| DEP-001 | Scala      | Upgrade from 2.11 to 2.12 or 2.13 |
| DEP-005 | Hadoop     | Upgrade from 2.x to 3.x           |
| DEP-006 | Python     | Upgrade from 2.7 to 3.6+          |
| DEP-007 | PyArrow    | Upgrade to 4.0+                   |
| DEP-008 | Pandas     | Upgrade to 1.0+                   |

### HDFS to S3 Migration (S3-xxx)

Storage transition patterns:

| Rule ID | Issue                 | Recommendation                          |
| ------- | --------------------- | --------------------------------------- |
| S3-001  | HDFS paths found      | Convert to s3a:// protocol              |
| S3-003  | s3:// or s3n:// used  | Use s3a:// for better performance       |
| S3-011  | Write operations      | Configure S3A committers                |
| S3-022  | Rename operations     | Avoid renames (expensive on S3)         |
| S3-023  | Append mode           | Use partitioned writes or table formats |
| S3-040  | Parquet files         | Consider Delta Lake/Iceberg for ACID    |
| S3-050  | Hardcoded credentials | Use IAM roles instead                   |

### Hive SET Commands to Remove (HIVE-xxx)

Legacy Hive properties that must be removed from SQL scripts:

| Rule ID  | Property Pattern         | Reason                                   |
| -------- | ------------------------ | ---------------------------------------- |
| HIVE-001 | hive.execution.engine    | Spark has its own engine                 |
| HIVE-002 | hive.exec.\*             | Hive executor settings                   |
| HIVE-003 | hive.mapred.\*           | MapReduce not used                       |
| HIVE-004 | hive.tez.\*              | Tez not used by Spark                    |
| HIVE-005 | hive.vectorized.\*       | Use Spark vectorization                  |
| HIVE-006 | hive.cbo.\*              | Use spark.sql.cbo.\*                     |
| HIVE-007 | hive.stats.\*            | Use ANALYZE TABLE                        |
| HIVE-008 | hive.optimize.\*         | Use Spark optimizer configs              |
| HIVE-010 | hive.auto.convert.join\* | Use spark.sql.autoBroadcastJoinThreshold |
| HIVE-013 | hive.skewjoin.\*         | Use spark.sql.adaptive.skewJoin.\*       |
| HIVE-018 | hive.txn.\*              | Use Delta Lake/Iceberg                   |
| HIVE-021 | hive.server2.\*          | Use Spark Thrift Server                  |
| HIVE-024 | hive.llap.\*             | LLAP not used by Spark                   |
| HIVE-100 | mapred.\*                | MapReduce not used                       |
| HIVE-101 | mapreduce.\*             | MapReduce not used                       |
| HIVE-103 | tez.\*                   | Tez not used                             |
| HIVE-110 | ADD JAR                  | Use --jars or spark.jars                 |
| HIVE-112 | dfs commands             | Use Spark APIs or shell                  |

### JDK 8/11 → 17 Migration (JDK-xxx)

Java platform evolution:

| Rule ID | Issue                            | Action Required                            |
| ------- | -------------------------------- | ------------------------------------------ |
| JDK-001 | sun.misc.Unsafe access           | Use VarHandle/MethodHandles or --add-opens |
| JDK-002 | sun.misc.BASE64Encoder           | Use java.util.Base64                       |
| JDK-003 | javax.xml.bind (JAXB)            | Add jakarta.xml.bind-api dependency        |
| JDK-004 | javax.activation                 | Add jakarta.activation dependency          |
| JDK-006 | javax.xml.ws (JAX-WS)            | Add jakarta.xml.ws-api dependency          |
| JDK-010 | Nashorn JavaScript               | Use GraalJS or alternative engine          |
| JDK-014 | Security Manager                 | Use container/OS sandboxing instead        |
| JDK-020 | sun.\* internal APIs             | Use supported APIs or --add-opens          |
| JDK-023 | setAccessible(true)              | Review reflective access needs             |
| JDK-030 | CMS GC (-XX:+UseConcMarkSweepGC) | Use G1GC or ZGC                            |
| JDK-035 | Legacy GC logging                | Use -Xlog:gc\* unified logging             |
| JDK-051 | finalize() method                | Use Cleaner or try-with-resources          |
| JDK-071 | --illegal-access flag            | Use specific --add-opens flags             |

### Python 2 → 3 Migration (PY-xxx)

Python language evolution:

| Rule ID | Issue                  | Action Required                |
| ------- | ---------------------- | ------------------------------ |
| PY-001  | print statement        | Use print() function           |
| PY-020  | unicode() builtin      | Use str() instead              |
| PY-030  | xrange()               | Use range()                    |
| PY-032  | dict.iteritems()       | Use dict.items()               |
| PY-033  | dict.has_key()         | Use 'key in dict'              |
| PY-041  | ConfigParser module    | Use configparser (lowercase)   |
| PY-044  | urllib/urllib2         | Use urllib.request/parse/error |
| PY-047  | StringIO module        | Use io.StringIO/BytesIO        |
| PY-050  | except E, e: syntax    | Use except E as e:             |
| PY-080  | raw_input()            | Use input()                    |
| PY-100  | Backtick repr          | Use repr()                     |
| PY-101  | <> operator            | Use !=                         |
| PY-102  | Octal literals         | Use 0o prefix (0o755)          |
| PY-110  | Lambda tuple unpacking | Use lambda x: (x[0], x[1])     |

## Example Output

### Markdown Report

```markdown
# Spark Migration Scan Report

## Summary

- **Files Scanned:** 156
- **Lines Scanned:** 24,892
- **Total Issues Found:** 47

### Issues by Severity

- 🔴 **CRITICAL:** 5
- 🟠 **HIGH:** 12
- 🟡 **MEDIUM:** 18
- 🔵 **LOW:** 8
- ℹ️ **INFO:** 4

## Detailed Findings

### Spark API Changes

#### 🔴 [SPARK-001] Removed: SQLContext

**Severity:** CRITICAL
**Description:** SQLContext was removed in Spark 3.0
**Suggestion:** Replace with SparkSession

**Occurrences:**

- `src/main/python/etl/processor.py:45`
```

sqlContext = SQLContext(sc)

```

```

### HTML Report

Generates a styled HTML page with color-coded severities, per-category sections, and occurrence listings with inline code snippets.

### JSON Report

```json
{
  "summary": {
    "files_scanned": 156,
    "lines_scanned": 24892,
    "total_issues": 47,
    "by_severity": {
      "critical": 5,
      "high": 12,
      "medium": 18
    },
    "by_category": {
      "Spark API Changes": 15,
      "HDFS to S3 Migration": 12
    }
  },
  "issues": [
    {
      "rule_id": "SPARK-001",
      "title": "Removed: SQLContext",
      "severity": "critical",
      "file_path": "src/main/python/etl/processor.py",
      "line_number": 45,
      "line_content": "sqlContext = SQLContext(sc)"
    }
  ]
}
```

## Testing

### Running the Test Suite

```bash
cd code-scanner
pip install -r requirements-test.txt
pytest tests/ -v
```

### Running Specific Test Modules

```bash
# Rule pattern tests only
pytest tests/test_rules.py -v

# Multi-line and AST-filtering tests
pytest tests/test_scanner_core.py -v -k "multiline or ast"

# CLI tests only
pytest tests/test_cli.py -v
```

### Coverage Report

```bash
pytest tests/ --cov=code-scanner --cov-report=html
open htmlcov/index.html
```

The suite enforces a minimum of **80% code coverage**. The CI pipeline will fail if coverage drops below this threshold.

### Test Dependencies

Declared in `requirements-test.txt`:

```
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.12.0
pytest-timeout>=2.2.0
```

No Airflow, PySpark, or other heavy runtime dependencies are needed — `code-scanner.py` is a pure-stdlib script.

### What Is Tested

| Test module            | Coverage area                                                                                                                                          |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `test_scanner_core.py` | `scan_file`, `scan_directory`, `scan_path`, multi-line detection, AST-aware filtering, deduplication, `get_summary`, all three report formats          |
| `test_rules.py`        | Positive and negative pattern matches for rules across all categories (SPARK, SQL, HIVE, S3, JDK, PY, PERF); rule-subset filtering via the constructor |
| `test_cli.py`          | `--severity`, `--category`, `--format`, `--output`, `--verbose`, `--quiet`, error exit codes; `ASTCodeExtractor` unit tests                            |

## CI/CD Integration

### GitHub Actions

The repository includes `.github/workflows/test.yml` which runs automatically on every push and pull request to `main` and `develop`. It runs the full test suite across Python 3.9, 3.10, and 3.11 in parallel, enforces 80% coverage, and runs `flake8` linting.

To use the scanner as an additional scan step in your own workflow:

```yaml
name: Spark Migration Check

on: [pull_request]

jobs:
  migration-scan:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.10"

      - name: Run Migration Scanner
        run: |
          python code-scanner.py . -f json -o migration-report.json -s high

      - name: Check for Critical Issues
        run: |
          CRITICAL=$(jq '.summary.by_severity.critical // 0' migration-report.json)
          if [ "$CRITICAL" -gt 0 ]; then
            echo "::error::Found $CRITICAL critical migration issues!"
            exit 1
          fi

      - name: Upload Report
        uses: actions/upload-artifact@v3
        with:
          name: migration-report
          path: migration-report.json
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Migration Scan') {
            steps {
                sh 'python code-scanner/code-scanner.py . -f json -o migration-report.json'
            }
        }
        stage('Check Results') {
            steps {
                script {
                    def report = readJSON file: 'migration-report.json'
                    def critical = report.summary.by_severity.critical ?: 0
                    if (critical > 0) {
                        error("Found ${critical} critical migration issues")
                    }
                }
            }
        }
    }
    post {
        always {
            archiveArtifacts artifacts: 'migration-report.json'
        }
    }
}
```

## Migration Checklist

### Pre-Migration Steps

- [ ] Run scanner on entire codebase
- [ ] Review all CRITICAL and HIGH severity issues
- [ ] Create tracking tickets for each issue
- [ ] Estimate remediation effort

### Spark 2.4 → 3.5 Migration

- [ ] **Dependencies**
  - [ ] Upgrade Scala to 2.12 or 2.13
  - [ ] Upgrade Java to 11 or 17 (recommended)
  - [ ] Upgrade Python to 3.8+ (recommended)
  - [ ] Update PyArrow to 4.0+
  - [ ] Update Pandas to 1.0+
  - [ ] Update all Spark packages to 3.x versions

- [ ] **Code Changes**
  - [ ] Replace SQLContext/HiveContext with SparkSession
  - [ ] Update deprecated DataFrame APIs
  - [ ] Add explicit types to all UDFs
  - [ ] Update Pandas UDF decorators
  - [ ] Migrate spark.mllib to spark.ml
  - [ ] Review window function frame specifications

- [ ] **SQL Changes**
  - [ ] Add explicit schemas to from_json calls
  - [ ] Add explicit formats to date/timestamp parsing
  - [ ] Review CAST operations for null handling
  - [ ] Update datetime format patterns
  - [ ] Add USING clause to CREATE TABLE statements

- [ ] **Configuration**
  - [ ] Enable Adaptive Query Execution (AQE)
  - [ ] Review datetime parser policy
  - [ ] Update Arrow configuration names
  - [ ] Remove deprecated configs

### Hive SQL Script Cleanup

- [ ] **Remove Execution Engine Properties**
  - [ ] Remove SET hive.execution.engine
  - [ ] Remove SET hive.exec.\* properties
  - [ ] Remove SET hive.mapred.\* properties
- [ ] **Remove Tez/LLAP Properties**
  - [ ] Remove SET hive.tez.\* properties
  - [ ] Remove SET hive.llap.\* properties
  - [ ] Remove SET tez.\* properties
- [ ] **Remove Optimizer Properties**
  - [ ] Remove SET hive.optimize.\* (use Spark AQE)
  - [ ] Remove SET hive.cbo._ (use spark.sql.cbo._)
  - [ ] Remove SET hive.vectorized.\* (Spark handles this)
  - [ ] Remove SET hive.auto.convert.join\* (use spark.sql.autoBroadcastJoinThreshold)
- [ ] **Remove MapReduce Properties**
  - [ ] Remove SET mapred.\* properties
  - [ ] Remove SET mapreduce.\* properties
  - [ ] Remove SET yarn.\* from SQL scripts
- [ ] **Remove Other Hive Properties**
  - [ ] Remove SET hive.stats.\* (use ANALYZE TABLE)
  - [ ] Remove SET hive.merge.\* (use coalesce/repartition)
  - [ ] Remove SET hive.txn.\* (use Delta/Iceberg)
  - [ ] Remove SET hive.compactor.\*
  - [ ] Remove SET hive.server2.\*
  - [ ] Remove SET hive.session.\*
- [ ] **Remove Hive CLI Commands**
  - [ ] Remove ADD JAR (use --jars or spark.jars)
  - [ ] Remove ADD FILE (use --files or spark.files)
  - [ ] Remove dfs commands
  - [ ] Remove source commands

### HDFS → S3 Migration

- [ ] **Path Changes**
  - [ ] Convert all hdfs:// to s3a://
  - [ ] Update s3:// and s3n:// to s3a://
  - [ ] Replace hardcoded paths with configurable variables

- [ ] **Configuration**
  - [ ] Configure S3A credentials (IAM roles preferred)
  - [ ] Set up S3A committers for reliable writes
  - [ ] Configure server-side encryption
  - [ ] Remove HDFS-specific configs (replication, block size)

- [ ] **Code Patterns**
  - [ ] Eliminate rename operations
  - [ ] Replace append mode with partitioned writes
  - [ ] Optimize for S3 access patterns (batch reads, avoid small files)
  - [ ] Consider Delta Lake/Iceberg for transactional workloads

- [ ] **Security**
  - [ ] Remove hardcoded credentials
  - [ ] Implement IAM roles for EC2/EKS
  - [ ] Configure KMS encryption if required

### JDK 8/11 → 17 Migration

- [ ] **Removed Java EE Modules**
  - [ ] Add jakarta.xml.bind-api for JAXB
  - [ ] Add jakarta.activation for javax.activation
  - [ ] Add jakarta.xml.ws-api for JAX-WS
  - [ ] Add jakarta.annotation-api for common annotations
  - [ ] Remove CORBA dependencies (migrate to gRPC/REST)

- [ ] **Internal API Access**
  - [ ] Replace sun.misc.Unsafe with VarHandle/MethodHandles
  - [ ] Replace sun.misc.BASE64Encoder with java.util.Base64
  - [ ] Document required --add-opens/--add-exports flags
  - [ ] Remove --illegal-access workarounds

- [ ] **JVM Flags**
  - [ ] Replace CMS GC with G1GC or ZGC
  - [ ] Update GC logging to -Xlog:gc\*
  - [ ] Remove PermSize flags (use MaxMetaspaceSize)
  - [ ] Remove deprecated flags (AggressiveOpts, etc.)

- [ ] **Code Updates**
  - [ ] Replace finalize() with Cleaner/try-with-resources
  - [ ] Replace Nashorn with GraalJS
  - [ ] Update reflection to handle encapsulation
  - [ ] Test locale-sensitive code

### Python 2 → 3 Migration

- [ ] **Syntax Changes**
  - [ ] Convert print statements to print()
  - [ ] Update except syntax (except E as e:)
  - [ ] Update raise syntax
  - [ ] Fix octal literals (0o755)
  - [ ] Remove <> operators (use !=)

- [ ] **Removed Builtins**
  - [ ] Replace unicode() with str()
  - [ ] Replace xrange() with range()
  - [ ] Replace raw_input() with input()
  - [ ] Replace execfile() with exec(open().read())
  - [ ] Replace reduce() with functools.reduce()
  - [ ] Replace apply() with func(\*args, \*\*kwargs)

- [ ] **Dictionary Changes**
  - [ ] Replace .iteritems() with .items()
  - [ ] Replace .iterkeys() with .keys()
  - [ ] Replace .itervalues() with .values()
  - [ ] Replace .has_key() with 'in' operator

- [ ] **Module Renames**
  - [ ] ConfigParser → configparser
  - [ ] Queue → queue
  - [ ] SocketServer → socketserver
  - [ ] urllib/urllib2 → urllib.request/parse/error
  - [ ] StringIO → io.StringIO
  - [ ] cPickle → pickle

- [ ] **String/Bytes**
  - [ ] Handle str vs bytes properly
  - [ ] Add explicit encoding to file operations
  - [ ] Update encode()/decode() calls

## Extending the Scanner

### Adding Custom Rules

```python
from spark_migration_scanner import ScanRule, Severity, Category
import re

# Define custom rule
custom_rule = ScanRule(
    rule_id="CUSTOM-001",
    title="Custom Pattern",
    description="Description of the issue",
    severity=Severity.HIGH,
    category=Category.SPARK_API,
    pattern=re.compile(r'your_pattern_here'),
    suggestion="How to fix the issue",
    documentation_url="https://docs.example.com"
)

# Add to scanner
from spark_migration_scanner import SparkMigrationScanner, ALL_RULES

custom_rules = ALL_RULES + [custom_rule]
scanner = SparkMigrationScanner(rules=custom_rules)
```

### Filtering Results Programmatically

```python
from spark_migration_scanner import SparkMigrationScanner, Severity, Category

scanner = SparkMigrationScanner()
scanner.scan_path('/path/to/project')

# Filter by severity
critical_issues = [i for i in scanner.issues if i.severity == Severity.CRITICAL]

# Filter by category
s3_issues = [i for i in scanner.issues if i.category == Category.HDFS_TO_S3]

# Filter by file path
python_issues = [i for i in scanner.issues if i.file_path.endswith('.py')]
```

## Known Limitations

1. **Static Analysis Only**: Cannot detect runtime-only issues or dynamic code patterns
2. **Pattern Matching**: May produce false positives in non-executable contexts (though `ast_aware` rules mitigate this for Python files)
3. **Context Unaware**: Cannot determine if deprecated API is used in legacy code paths
4. **No Type Analysis**: Cannot verify type compatibility changes without running code
5. **Multi-line size limit**: Multi-line scanning is skipped for files larger than 1 MB

## Resources

### Spark Migration Guides

- [Spark 3.0 Migration Guide](https://spark.apache.org/docs/3.0.0/sql-migration-guide.html)
- [Spark 3.5 Release Notes](https://spark.apache.org/docs/3.5.0/sql-migration-guide.html)
- [PySpark Migration Guide](https://spark.apache.org/docs/3.5.0/api/python/migration_guide/pyspark_upgrade.html)

### S3 Integration

- [Hadoop AWS Module](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [S3A Committers](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/committers.html)
- [Delta Lake on S3](https://docs.delta.io/latest/delta-storage.html#amazon-s3)
- [Iceberg on S3](https://iceberg.apache.org/docs/latest/aws/)

## License

Apache 2.0
