"""
Pure-Python partition filtering and SQL clause helpers.
"""

import re
import urllib


def apply_partition_filter(partitions, filter_expr):
    """Filter Hive partition strings against a filter expression."""

    if not filter_expr:
        return partitions

    def parse_partition(part_str):
        result = {}
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                result[k.strip()] = urllib.parse.unquote(v.strip())
        return result

    def try_numeric(val):
        try:
            return int(val)
        except ValueError:
            return val

    terms = [t.strip() for t in filter_expr.split(',') if t.strip()]
    matched = set()

    for term in terms:
        m = re.match(r'^last_n_partitions=(\d+)$', term.strip())
        if m:
            for p in sorted(partitions, reverse=True)[:int(m.group(1))]:
                matched.add(p)
            continue

        if not any(op in term for op in ('>=', '<=', '>', '<')):
            if term.endswith('/*') or term.endswith('*'):
                prefix = term.rstrip('*').rstrip('/')
                for p in partitions:
                    if p.startswith(prefix):
                        matched.add(p)
            else:
                for p in partitions:
                    if p == term:
                        matched.add(p)
            continue

        op_match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(>=|<=|>|<)(.+)$', term.strip())
        if op_match:
            key = op_match.group(1)
            op = op_match.group(2)
            threshold_raw = op_match.group(3).strip()
            threshold_is_prefix = threshold_raw.endswith('*')
            threshold = threshold_raw.rstrip('*')
            threshold_cmp = try_numeric(threshold)
            for p in partitions:
                pdict = parse_partition(p)
                if key not in pdict:
                    continue
                pval = pdict[key]
                pval_for_cmp = pval[:len(threshold)] if threshold_is_prefix else pval
                pval_cmp = try_numeric(pval_for_cmp)
                try:
                    if (  # noqa: SIM114
                        (op == '>=' and pval_cmp >= threshold_cmp)
                        or (op == '<=' and pval_cmp <= threshold_cmp)
                        or (op == '>'  and pval_cmp >  threshold_cmp)
                        or (op == '<'  and pval_cmp <  threshold_cmp)
                    ):
                        matched.add(p)
                except TypeError:
                    if (  # noqa: SIM114
                        (op == '>=' and pval_for_cmp >= threshold)
                        or (op == '<=' and pval_for_cmp <= threshold)
                        or (op == '>'  and pval_for_cmp >  threshold)
                        or (op == '<'  and pval_for_cmp <  threshold)
                    ):
                        matched.add(p)
            continue

    return [p for p in partitions if p in matched]


def partitions_to_where_clause(partitions):
    """Convert partition strings to a SQL WHERE clause. Values are single-quote escaped."""
    if not partitions:
        return "1=0"
    clauses = []
    for part_str in partitions:
        conditions = []
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                conditions.append("`%s`='%s'" % (k.strip(), v.replace("'", "''").strip()))
        if conditions:
            clauses.append("(" + " AND ".join(conditions) + ")")
    return " OR ".join(clauses) if clauses else "1=1"
