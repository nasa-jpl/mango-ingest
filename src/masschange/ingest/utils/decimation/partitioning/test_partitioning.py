import unittest

from masschange.ingest.utils.decimation.partitioning import get_partition_id


class GetKeyTestCase(unittest.TestCase):
    def test_produces_correct_keys(self):
        self.assertEqual(3600, get_partition_id(5400, hours_per_partition=1, epoch_unit_factor=1))
        self.assertEqual(0, get_partition_id(5400, hours_per_partition=2, epoch_unit_factor=1))
        self.assertEqual(0, get_partition_id(5400, hours_per_partition=1))
        self.assertEqual(3600000000, get_partition_id(5400000000, hours_per_partition=1))


if __name__ == '__main__':
    unittest.main()
