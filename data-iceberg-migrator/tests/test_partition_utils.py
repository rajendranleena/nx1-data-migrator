"""
Tests for utils/migrations/partition_utils.py
"""

from utils.migrations import partition_utils as pu


# ---------------------------------------------------------------------------
# apply_partition_filter
# ---------------------------------------------------------------------------
class TestApplyPartitionFilter:

    PARTS = [
        'dt=2024-01-01',
        'dt=2024-01-02',
        'dt=2024-01-03',
        'year=2023/month=11',
        'year=2023/month=12',
        'year=2024/month=01',
    ]

    def test_empty_filter_returns_all_partitions(self):
        assert pu.apply_partition_filter(self.PARTS, '') == self.PARTS

    def test_none_filter_returns_all_partitions(self):
        assert pu.apply_partition_filter(self.PARTS, None) == self.PARTS

    def test_empty_partition_list_returns_empty(self):
        assert pu.apply_partition_filter([], 'dt=2024-01-01') == []

    def test_exact_match(self):
        result = pu.apply_partition_filter(self.PARTS, 'dt=2024-01-02')
        assert result == ['dt=2024-01-02']

    def test_exact_match_uses_raw_partition_string(self):
        """Exact match is against the raw encoded string, not the decoded value."""
        parts = ['city=New%20York', 'city=London']
        result = pu.apply_partition_filter(parts, 'city=London')
        assert result == ['city=London']

    def test_no_match_returns_empty_list(self):
        result = pu.apply_partition_filter(self.PARTS, 'dt=1999-01-01')
        assert result == []

    def test_wildcard_prefix_match_with_trailing_slash(self):
        result = pu.apply_partition_filter(self.PARTS, 'year=2023/*')
        assert set(result) == {'year=2023/month=11', 'year=2023/month=12'}

    def test_wildcard_without_slash_matches_prefix(self):
        parts = ['region=us/dt=2024-01', 'region=us/dt=2024-02', 'region=eu/dt=2024-01']
        result = pu.apply_partition_filter(parts, 'region=us*')
        assert set(result) == {'region=us/dt=2024-01', 'region=us/dt=2024-02'}

    def test_last_n_partitions(self):
        result = pu.apply_partition_filter(self.PARTS, 'last_n_partitions=2')
        assert len(result) == 2
        assert 'year=2024/month=01' in result

    def test_last_n_larger_than_partition_count_returns_all(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-02']
        result = pu.apply_partition_filter(parts, 'last_n_partitions=100')
        assert set(result) == set(parts)

    def test_last_n_of_one(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03']
        result = pu.apply_partition_filter(parts, 'last_n_partitions=1')
        assert len(result) == 1
        assert 'dt=2024-01-03' in result

    def test_greater_than_operator(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-03', 'dt=2024-01-05']
        result = pu.apply_partition_filter(parts, 'dt>2024-01-02')
        assert set(result) == {'dt=2024-01-03', 'dt=2024-01-05'}

    def test_greater_than_or_equal_operator(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03']
        result = pu.apply_partition_filter(parts, 'dt>=2024-01-02')
        assert set(result) == {'dt=2024-01-02', 'dt=2024-01-03'}

    def test_less_than_operator(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03']
        result = pu.apply_partition_filter(parts, 'dt<2024-01-02')
        assert result == ['dt=2024-01-01']

    def test_less_than_or_equal_operator(self):
        parts = ['year=2022/month=06', 'year=2023/month=01', 'year=2024/month=01']
        result = pu.apply_partition_filter(parts, 'year<=2023')
        assert set(result) == {'year=2022/month=06', 'year=2023/month=01'}

    def test_numeric_integer_comparison(self):
        parts = ['partition_id=5', 'partition_id=10', 'partition_id=15']
        result = pu.apply_partition_filter(parts, 'partition_id>=10')
        assert set(result) == {'partition_id=10', 'partition_id=15'}

    def test_string_fallback_comparison_on_type_error(self):
        """Non-numeric values fall back to lexicographic string comparison."""
        parts = ['ver=a1', 'ver=b2', 'ver=c3']
        result = pu.apply_partition_filter(parts, 'ver>=b2')
        assert set(result) == {'ver=b2', 'ver=c3'}

    def test_filter_key_absent_from_partition_is_skipped(self):
        """Comparison against a key that does not exist in a partition skips that partition."""
        parts = ['dt=2024-01-01', 'dt=2024-01-02']
        result = pu.apply_partition_filter(parts, 'month>=01')
        assert result == []

    def test_multiple_comma_separated_terms(self):
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03']
        result = pu.apply_partition_filter(parts, 'dt=2024-01-01, dt=2024-01-03')
        assert set(result) == {'dt=2024-01-01', 'dt=2024-01-03'}

    def test_mixed_term_types_in_one_expression(self):
        """Combine last_n with an exact match — union of both sets returned."""
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03', 'dt=2024-01-04']
        result = pu.apply_partition_filter(parts, 'dt=2024-01-01, last_n_partitions=1')
        assert 'dt=2024-01-01' in result
        assert 'dt=2024-01-04' in result

    def test_unrecognised_term_is_skipped(self):
        result = pu.apply_partition_filter(['dt=2024-01-01'], 'garbage!!term')
        assert result == []

    def test_output_preserves_original_partition_order(self):
        """Result must follow the original list order, not insertion/match order."""
        parts = ['dt=2024-01-03', 'dt=2024-01-01', 'dt=2024-01-02']
        result = pu.apply_partition_filter(parts, 'dt=2024-01-03, dt=2024-01-01')
        assert result == ['dt=2024-01-03', 'dt=2024-01-01']


# ---------------------------------------------------------------------------
# partitions_to_where_clause
# ---------------------------------------------------------------------------
class TestPartitionsToWhereClause:

    def test_empty_list_returns_false_clause(self):
        assert pu.partitions_to_where_clause([]) == '1=0'

    def test_single_partition_single_key(self):
        result = pu.partitions_to_where_clause(['dt=2024-01-01'])
        assert result == "(`dt`='2024-01-01')"

    def test_single_partition_multi_key(self):
        result = pu.partitions_to_where_clause(['year=2024/month=01'])
        assert result == "(`year`='2024' AND `month`='01')"

    def test_multiple_partitions_joined_with_or(self):
        result = pu.partitions_to_where_clause(['dt=2024-01-01', 'dt=2024-01-02'])
        assert "(`dt`='2024-01-01')" in result
        assert "(`dt`='2024-01-02')" in result
        assert ' OR ' in result

    def test_segment_without_equals_sign_is_ignored(self):
        result = pu.partitions_to_where_clause(['no_equals_here'])
        assert result == '1=1'

    def test_three_key_partition(self):
        result = pu.partitions_to_where_clause(['year=2024/month=01/day=15'])
        assert "`year`='2024'" in result
        assert "`month`='01'" in result
        assert "`day`='15'" in result
        assert 'AND' in result

    def test_apostrophe_in_value_is_escaped(self):
        result = pu.partitions_to_where_clause(["city=O'Brien"])
        assert "O''Brien" in result
        sanitised = result.replace("O''Brien", "")
        assert "O'Brien" not in sanitised

    def test_column_names_are_backtick_quoted(self):
        """Column names must be wrapped in backticks to handle reserved words."""
        result = pu.partitions_to_where_clause(['dt=2024-01-01'])
        assert '`dt`' in result

    def test_values_are_single_quoted(self):
        result = pu.partitions_to_where_clause(['region=us-east-1'])
        assert "'us-east-1'" in result

    def test_multiple_partitions_count(self):
        """Each partition produces exactly one clause; OR count == len(partitions) - 1."""
        parts = ['dt=2024-01-01', 'dt=2024-01-02', 'dt=2024-01-03']
        result = pu.partitions_to_where_clause(parts)
        assert result.count(' OR ') == 2

    def test_whitespace_trimmed_from_keys_and_values(self):
        """Leading/trailing whitespace in segment key/value must be stripped."""
        result = pu.partitions_to_where_clause(['  dt = 2024-01-01 '])
        assert '`dt`' in result
        assert '2024-01-01' in result
