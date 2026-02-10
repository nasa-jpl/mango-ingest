import unittest

from masschange.dataproducts.utils import get_dataproduct_classes
from masschange.ingest.executor.datafilereaders.baseevents import EventsFileReader
from masschange.ingest.executor.datafilereaders.base_offred import OffredFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.ddic import GraceFODdicDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.icsnr import GraceFOIcsnrDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_ang_x import GraceFOAcc1bAngXDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_ang_y import GraceFOAcc1bAngYDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_ang_z import GraceFOAcc1bAngZDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_lin_x import GraceFOAcc1bLinXDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_lin_y import GraceFOAcc1bLinYDataFileReader
from masschange.ingest.executor.datafilereaders.gracefo.primary.acc1b_lin_z import GraceFOAcc1bLinZDataFileReader

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
        dataset_implementations = get_dataproduct_classes()
        readers_without_versions = \
            (OffredFileReader, GraceFODdicDataFileReader, GraceFOIcsnrDataFileReader,
             GraceFOAcc1bAngXDataFileReader, GraceFOAcc1bAngYDataFileReader, GraceFOAcc1bAngZDataFileReader,
             GraceFOAcc1bLinXDataFileReader, GraceFOAcc1bLinYDataFileReader, GraceFOAcc1bLinZDataFileReader)
        for implementation in dataset_implementations:
            if isinstance(implementation.get_reader(), readers_without_versions):
                mandatory_capture_group_names = {
                    'instrument_id',
                }
            else:
                mandatory_capture_group_names = {
                    'instrument_id',
                    'dataset_version'
                }

            for capture_group_name in mandatory_capture_group_names:
                try:
                    self.assertIn(f'(?P<{capture_group_name}>', implementation.get_reader().get_input_file_default_regex())
                except AssertionError:
                    raise NotImplementedError(
                        f'Capture group "{capture_group_name}" not implemented in {implementation.__name__}.get_input_file_default_regex()')


if __name__ == '__main__':
    unittest.main()
