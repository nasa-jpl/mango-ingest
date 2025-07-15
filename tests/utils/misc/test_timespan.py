import unittest
from datetime import datetime, timedelta

from masschange.utils.timespan import TimeSpan


class TimeSpanTestCase(unittest.TestCase):

    def test_intersects(self):
        ts_ref = TimeSpan(begin=datetime(2020, 1, 1), end=datetime(2020, 1, 2))
        offset = timedelta(hours=1)

        ts_strict_subset = TimeSpan(begin=ts_ref.begin + offset, end=ts_ref.end - offset)
        ts_left_overlap = TimeSpan(begin=ts_ref.begin - offset, end=ts_ref.end - offset)
        ts_right_overlap = TimeSpan(begin=ts_ref.begin + offset, end=ts_ref.end + offset)
        ts_strict_superset = TimeSpan(begin=ts_ref.begin - offset, end=ts_ref.end + offset)
        ts_disjoint = TimeSpan(begin=ts_ref.end + offset, end=ts_ref.end + 2*offset)

        strict_subset_intersection = ts_ref.intersection(ts_strict_subset)
        left_overlap_intersection = ts_ref.intersection(ts_left_overlap)
        right_overlap_intersection = ts_ref.intersection(ts_right_overlap)
        strict_superset_intersection = ts_ref.intersection(ts_strict_superset)
        ts_disjoint_intersection = ts_ref.intersection(ts_disjoint)

        expected_strict_subset_intersection = ts_strict_subset
        expected_left_overlap_intersection = TimeSpan(begin=ts_ref.begin, end=ts_left_overlap.end)
        expected_right_overlap_intersection = TimeSpan(begin=ts_right_overlap.begin, end=ts_ref.end)
        expected_strict_superset_intersection = ts_ref

        self.assertEqual(expected_strict_subset_intersection, strict_subset_intersection)
        self.assertEqual(expected_left_overlap_intersection, left_overlap_intersection)
        self.assertEqual(expected_right_overlap_intersection, right_overlap_intersection)
        self.assertEqual(expected_strict_superset_intersection, strict_superset_intersection)
        self.assertIsNone(ts_disjoint_intersection)
        
    def test_unions(self):
        ts_ref = TimeSpan(begin=datetime(2020, 1, 1), end=datetime(2020, 1, 2))
        offset = timedelta(hours=1)

        ts_strict_subset = TimeSpan(begin=ts_ref.begin + offset, end=ts_ref.end - offset)
        ts_left_overlap = TimeSpan(begin=ts_ref.begin - offset, end=ts_ref.end - offset)
        ts_right_overlap = TimeSpan(begin=ts_ref.begin + offset, end=ts_ref.end + offset)
        ts_strict_superset = TimeSpan(begin=ts_ref.begin - offset, end=ts_ref.end + offset)
        ts_disjoint = TimeSpan(begin=ts_ref.end + offset, end=ts_ref.end + 2*offset)


        strict_subset_union = ts_ref.union(ts_strict_subset)
        left_overlap_union = ts_ref.union(ts_left_overlap)
        right_overlap_union = ts_ref.union(ts_right_overlap)
        strict_superset_union = ts_ref.union(ts_strict_superset)

        expected_strict_subset_union = ts_ref
        expected_left_overlap_union = TimeSpan(begin=ts_left_overlap.begin, end=ts_ref.end)
        expected_right_overlap_union = TimeSpan(begin=ts_ref.begin, end=ts_right_overlap.end)
        expected_strict_superset_union = ts_strict_superset
        expected_disjoint_union = TimeSpan(begin=ts_ref.begin, end=ts_disjoint.end)

        self.assertEqual(expected_strict_subset_union, strict_subset_union)
        self.assertEqual(expected_left_overlap_union, left_overlap_union)
        self.assertEqual(expected_right_overlap_union, right_overlap_union)
        self.assertEqual(expected_strict_superset_union, strict_superset_union)

        self.assertRaises(ValueError, lambda : ts_ref.union(ts_disjoint))
        self.assertEqual(expected_disjoint_union, ts_ref.union(ts_disjoint, allow_disconnected_spans=True))
