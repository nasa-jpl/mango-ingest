import unittest

from masschange.datasets.utils import get_time_series_dataset_classes


class TestTimeSeriesDatasetImplementations(unittest.TestCase):
    def test_all_mandatory_attributes_defined(self):
        dataset_implementations = get_time_series_dataset_classes()
        for implementation in dataset_implementations:
            try:
                self.assertIsNotNone(implementation.mission)
                self.assertIsNotNone(implementation.id_suffix)
                self.assertLess(0, len(implementation.stream_ids))
                self.assertIsNotNone(implementation.time_series_interval)
            except AttributeError as err:
                raise NotImplementedError(str(err))


if __name__ == '__main__':
    unittest.main()
