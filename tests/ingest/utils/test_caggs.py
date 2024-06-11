import unittest

from masschange.ingest.utils.caggs import TrivialAggregation, NestedAggregation


class AggregationsTestCase(unittest.TestCase):

    def test_trivial_aggregation(self):
        agg = TrivialAggregation('MIN')
        self.assertEqual('MIN(someColumnName)', agg.get_sql_expression('someColumnName'))
        self.assertEqual('someColumnName_min', agg.get_aggregated_name('someColumnName'))

    def test_nested_aggregation(self):
        agg = NestedAggregation(['INNER_F', 'MIDDLE_F', 'OUTER_F'])
        self.assertEqual('OUTER_F(MIDDLE_F(INNER_F(someColumnName)))', agg.get_sql_expression('someColumnName'))
        self.assertEqual('someColumnName', agg.get_aggregated_name('someColumnName'))

    def test_nested_aggregation_override_output_name(self):
        agg = NestedAggregation(['F'], output_name_f_override=lambda column_name: f'someFunctionOf({column_name})')
        self.assertEqual('someFunctionOf(someColumnName)', agg.get_aggregated_name('someColumnName'))
