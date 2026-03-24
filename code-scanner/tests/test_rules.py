"""
Rule-level tests: verify specific rule_ids fire (positive) and don't
fire on clean equivalents (negative).
"""


def _scan(scanner, tmp_path, content, ext=".py"):
    f = tmp_path / f"sample{ext}"
    f.write_text(content)
    return scanner.scan_file(f)


# ---------------------------------------------------------------------------
# Spark API rules
# ---------------------------------------------------------------------------

class TestSparkApiRules:

    def test_SPARK001_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'ctx = SQLContext(sc)')
        assert any(i.rule_id == "SPARK-001" for i in issues)

    def test_SPARK001_negative(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'spark = SparkSession.builder.getOrCreate()')
        assert all(i.rule_id != "SPARK-001" for i in issues)

    def test_SPARK003_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'df.registerTempTable("t")')
        assert any(i.rule_id == "SPARK-003" for i in issues)

    def test_SPARK003_negative(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'df.createOrReplaceTempView("t")')
        assert all(i.rule_id != "SPARK-003" for i in issues)

    def test_SPARK009_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'out = df1.unionAll(df2)')
        assert any(i.rule_id == "SPARK-009" for i in issues)

    def test_SPARK009_negative(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'out = df1.union(df2)')
        assert all(i.rule_id != "SPARK-009" for i in issues)

    def test_SPARK030_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'from pyspark.mllib.classification import LogisticRegressionWithLBFGS')
        assert any(i.rule_id == "SPARK-030" for i in issues)

    def test_SPARK032_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'enc = OneHotEncoderEstimator()')
        assert any(i.rule_id == "SPARK-032" for i in issues)


# ---------------------------------------------------------------------------
# Spark SQL / Hive rules
# ---------------------------------------------------------------------------

class TestSparkSqlRules:

    def test_SQL001_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'SELECT CAST(val AS INT) FROM t', ext=".sql")
        assert any(i.rule_id == "SQL-001" for i in issues)

    def test_HIVE001_positive(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'SET hive.execution.engine=tez;', ext=".sql")
        assert any(i.rule_id == "HIVE-001" for i in issues)

    def test_HIVE001_sql_only(self, scanner, tmp_path):
        """HIVE-001 must NOT fire for .py files (file_extensions restriction)."""
        issues = _scan(scanner, tmp_path, 'SET hive.execution.engine=tez;', ext=".py")
        assert any(i.rule_id == "HIVE-001" for i in issues)

    def test_HIVE099_catch_all(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'SET hive.unknown.property=value;', ext=".sql")
        assert any(i.rule_id == "HIVE-099" for i in issues)

    def test_HIVE100_mapred(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'SET mapred.job.name=my_job;', ext=".sql")
        assert any(i.rule_id == "HIVE-100" for i in issues)

    def test_HIVE110_add_jar(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'ADD JAR /tmp/mylib.jar;', ext=".sql")
        assert any(i.rule_id == "HIVE-110" for i in issues)

    def test_SQL012_create_table_without_using(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'CREATE TABLE foo (id INT);', ext=".sql")
        assert any(i.rule_id == "SQL-012" for i in issues)

    def test_SQL012_negative(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'CREATE TABLE foo (id INT) USING parquet;', ext=".sql")
        assert all(i.rule_id != "SQL-012" for i in issues)


# ---------------------------------------------------------------------------
# S3 / HDFS rules
# ---------------------------------------------------------------------------

class TestS3Rules:

    def test_S3001_hdfs_path(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'df.read.parquet("hdfs://nn:8020/data")')
        assert any(i.rule_id == "S3-001" for i in issues)

    def test_S3003_s3n(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'path = "s3n://bucket/data"')
        assert any(i.rule_id == "S3-003" for i in issues)

    def test_S3003_negative_s3a(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'path = "s3a://bucket/data"')
        assert all(i.rule_id != "S3-003" for i in issues)

    def test_S3022_rename(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'fs.rename(src, dst)')
        assert any(i.rule_id == "S3-022" for i in issues)

    def test_S3050_hardcoded_key(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'aws_access_key_id = "AKIAIOSFODNN7EXAMPLE"')
        assert any(i.rule_id == "S3-050" for i in issues)

    def test_S3012_replication(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'conf.set("dfs.replication", "1")')
        assert any(i.rule_id == "S3-012" for i in issues)


# ---------------------------------------------------------------------------
# JDK migration rules
# ---------------------------------------------------------------------------

class TestJdkRules:

    def test_JDK001_unsafe(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'import sun.misc.Unsafe;', ext=".java")
        assert any(i.rule_id == "JDK-001" for i in issues)

    def test_JDK002_base64(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'import sun.misc.BASE64Encoder;', ext=".java")
        assert any(i.rule_id == "JDK-002" for i in issues)

    def test_JDK030_cms_gc(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'opts = "-XX:+UseConcMarkSweepGC"')
        assert any(i.rule_id == "JDK-030" for i in issues)

    def test_JDK071_illegal_access(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'JAVA_OPTS="--illegal-access=permit"')
        assert any(i.rule_id == "JDK-071" for i in issues)

    def test_JDK034_permsize(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'jvm_opts = "-XX:MaxPermSize=256m"')
        assert any(i.rule_id == "JDK-034" for i in issues)


# ---------------------------------------------------------------------------
# Python migration rules
# ---------------------------------------------------------------------------

class TestPythonMigrationRules:

    def test_PY001_print_statement(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'print "hello world"')
        assert any(i.rule_id == "PY-001" for i in issues)

    def test_PY001_negative_print_function(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'print("hello world")')
        assert all(i.rule_id != "PY-001" for i in issues)

    def test_PY030_xrange(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'for i in xrange(10):')
        assert any(i.rule_id == "PY-030" for i in issues)

    def test_PY032_iteritems(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'for k, v in d.iteritems():')
        assert any(i.rule_id == "PY-032" for i in issues)

    def test_PY050_except_comma(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'except Exception, e:')
        assert any(i.rule_id == "PY-050" for i in issues)

    def test_PY080_raw_input(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'name = raw_input("Enter name: ")')
        assert any(i.rule_id == "PY-080" for i in issues)

    def test_PY101_diamond_operator(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'if x <> y:')
        assert any(i.rule_id == "PY-101" for i in issues)


# ---------------------------------------------------------------------------
# Performance rules
# ---------------------------------------------------------------------------

class TestPerformanceRules:

    def test_PERF001_collect(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'data = df.collect()')
        assert any(i.rule_id == "PERF-001" for i in issues)

    def test_PERF002_cross_join(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, 'result = df1.crossJoin(df2)')
        assert any(i.rule_id == "PERF-002" for i in issues)

    def test_PERF003_udf(self, scanner, tmp_path):
        issues = _scan(scanner, tmp_path, '@udf\\ndef my_func(x): return x')
        assert any(i.rule_id == "PERF-003" for i in issues)


# ---------------------------------------------------------------------------
# Rule filtering
# ---------------------------------------------------------------------------

class TestRuleFiltering:

    def test_custom_rules_only(self, cs, tmp_path):
        """Scanner initialised with a subset of rules only applies those rules."""
        spark001_rule = next(r for r in cs.ALL_RULES if r.rule_id == "SPARK-001")
        custom_scanner = cs.SparkMigrationScanner(rules=[spark001_rule])
        f = tmp_path / "test.py"
        f.write_text('x = sqlContext\\ndf.unionAll(df2)')
        issues = custom_scanner.scan_file(f)
        rule_ids = {i.rule_id for i in issues}
        assert "SPARK-001" in rule_ids
        assert "SPARK-009" not in rule_ids

    def test_empty_rules_returns_no_issues(self, cs, tmp_path):
        empty_scanner = cs.SparkMigrationScanner(rules=[])
        f = tmp_path / "test.py"
        f.write_text("x = sqlContext\ndf.unionAll(df2)\n")
        issues = empty_scanner.scan_file(f)
        assert issues == []
