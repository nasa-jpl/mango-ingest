import unittest

from masschange.dataproducts.utils import get_dataproduct_classes
from masschange.ingest.executor.datafilereaders.baseevents import EventsFileReader

class TestTimeSeriesDatasetImplementations(unittest.TestCase):
    def test_all_mandatory_attributes_defined(self):
        dataset_implementations = get_dataproduct_classes()
        for implementation in dataset_implementations:
            try:
                self.assertIsNotNone(implementation.mission)
                self.assertIsNotNone(implementation.id_suffix)
                self.assertLess(0, len(implementation.instrument_ids))
                if not isinstance(implementation.get_reader(), EventsFileReader):
                    self.assertIsNotNone(implementation.processing_level)
                else:
                    self.assertIsNone(implementation.processing_level)
                if implementation.is_time_series_dataproduct():
                    self.assertIsNotNone(implementation.time_series_interval)
            except AttributeError as err:
                raise NotImplementedError(str(err))

    def test_mandatory_filename_regex_capture_groups(self):
        mandatory_capture_group_names = {
            'instrument_id',
            'dataset_version'
        }
        dataset_implementations = get_dataproduct_classes()
        for implementation in dataset_implementations:
            for capture_group_name in mandatory_capture_group_names:
                try:
                    self.assertIn(f'(?P<{capture_group_name}>', implementation.get_reader().get_input_file_default_regex())
                except AssertionError:
                    raise NotImplementedError(
                        f'Capture group "{capture_group_name}" not implemented in {implementation.__name__}.get_input_file_default_regex()')


if __name__ == '__main__':
    unittest.main()
